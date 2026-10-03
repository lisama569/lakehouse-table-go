package table

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// 跨进程测试通过重入测试二进制实现：
//   - 父进程 TestCrossProcessConcurrentCommit 编译自身并启动 N 个 worker 子进程；
//   - 子进程（环境变量 TABLE_XPROC_WORKER=1）运行 workerMain 后立即退出；
//   - 多个真正的 OS 进程同时打开同一张表、基于同一份 HEAD 提交。
//
// 覆盖点：恰好 1 个提交生效、其余收到可区分的 ErrCommitConflict；
// 失败方基于最新 HEAD 重试后全部落库，无重复行、无丢更新、成功提交不被覆盖。

type workerResult struct {
	Worker int    `json:"worker"`
	Phase  string `json:"phase"`
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Kind   string `json:"kind"`
	Head   string `json:"head"`
	Snap   string `json:"snap"`
	Rows   int    `json:"rows"`
}

func TestCrossProcessConcurrentCommit(t *testing.T) {
	if os.Getenv("TABLE_XPROC_WORKER") == "1" {
		workerMain(t)
		return
	}

	// 准备共享表目录并建表（父进程建好初始快照）。
	root := t.TempDir()
	store := NewStore(root)
	tbl, err := CreateTable(store, "orders", testSchema())
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	head, err := tbl.Head()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("场景: 跨进程并发提交，共享目录=%s 初始 snapshot=%s", root, head)

	const workers = 6
	bin := buildWorkerBinary(t)

	// ---- 阶段1：N 个 OS 进程并发基于同一父快照提交 ----
	results := runWorkers(t, bin, root, "attempt", workers, head)
	success, conflict, other := summarize(results)
	t.Logf("首轮跨进程提交结果: success=%d conflict=%d other=%d", success, conflict, other)
	if other != 0 {
		for _, r := range results {
			if r.Kind != "" && r.Kind != "conflict" {
				t.Errorf("worker %d unexpected error: %s", r.Worker, r.Error)
			}
		}
	}
	if success != 1 {
		t.Fatalf("首轮要求恰好 1 个进程成功，实际 %d", success)
	}
	if conflict != workers-1 {
		t.Fatalf("首轮要求 %d 个进程冲突失败，实际 %d", workers-1, conflict)
	}
	if rows := countRows(t, tbl); rows != 1 {
		t.Fatalf("失败提交不得部分生效，期望 1 行，实际 %d", rows)
	}

	// ---- 阶段2：冲突进程基于最新 HEAD 重试，各自不同批次，全部成功 ----
	cur, _ := tbl.Head()
	t.Logf("冲突进程以最新 HEAD=%s 重试", cur)
	retry := runWorkers(t, bin, root, "retry", workers, "")
	var rSuccess, rDup, rOther int
	for _, r := range retry {
		switch r.Kind {
		case "":
			rSuccess++
		case "duplicate":
			rDup++
		default:
			rOther++
		}
	}
	t.Logf("重试结果: success=%d duplicate=%d other=%d", rSuccess, rDup, rOther)
	if rOther != 0 {
		t.Fatalf("重试不应出现冲突以外的错误，other=%d", rOther)
	}

	// ---- 最终校验：无重复行、无丢更新、成功提交不被覆盖 ----
	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	wantRows := workers // 首轮 1 行 + 重试补齐其余（每进程一行唯一主键）
	if len(rows) != wantRows {
		t.Fatalf("重试后期望 %d 行，实际 %d（重复或丢更新）", wantRows, len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		pk := r["id"].(string)
		if seen[pk] {
			t.Fatalf("出现重复行: %s", pk)
		}
		seen[pk] = true
		// 值未被其他进程覆盖：name/score 与主键编号一致。
		n, _ := strconv.Atoi(strings.TrimPrefix(pk, "u"))
		if r["name"] != fmt.Sprintf("user%d", n) {
			t.Fatalf("行 %s 被覆盖: %v", pk, r)
		}
	}
	snapshots, _ := tbl.ReadSince(0)
	finalHead, _ := tbl.Head()
	t.Logf("最终 snapshot=%s 行数=%d 快照流水=%d 个，无重复无丢更新",
		finalHead, len(rows), len(snapshots))
	for _, s := range snapshots {
		t.Logf("快照流水 seq=%d id=%s parent=%s batch=%s", s.Seq, s.ID, s.ParentID, s.BatchID)
	}
}

// TestMultipleHandlesInProcess 同一进程内多个独立 Table 句柄并发提交，
// 模拟同机多实例，验证恰好一个成功、其余可区分失败、重试完整。
func TestMultipleHandlesInProcess(t *testing.T) {
	root := t.TempDir()
	st := NewStore(root)
	if _, err := CreateTable(st, "t", testSchema()); err != nil {
		t.Fatal(err)
	}
	const n = 5
	handles := make([]*Table, n)
	for i := range handles {
		h, err := OpenTable(st, "t")
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = h
	}
	head, _ := handles[0].Head()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := Batch{BatchID: fmt.Sprintf("mp-%d", i), Changes: []Change{
				{Upsert: &Row{"id": fmt.Sprintf("u%d", i), "name": fmt.Sprintf("user%d", i), "score": int64(i)}},
			}}
			_, errs[i] = handles[i].Commit(head, b)
		}(i)
	}
	wg.Wait()
	ok, conflict := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			ok++
		case errors.Is(e, ErrCommitConflict):
			conflict++
		default:
			t.Fatalf("unexpected: %v", e)
		}
	}
	t.Logf("多句柄并发: success=%d conflict=%d", ok, conflict)
	if ok != 1 || conflict != n-1 {
		t.Fatalf("want 1 ok and %d conflict, got %d/%d", n-1, ok, conflict)
	}
	// 冲突句柄重试落库。
	for i, e := range errs {
		if errors.Is(e, ErrCommitConflict) {
			cur, _ := handles[i].Head()
			b := Batch{BatchID: fmt.Sprintf("mp-%d", i), Changes: []Change{
				{Upsert: &Row{"id": fmt.Sprintf("u%d", i), "name": fmt.Sprintf("user%d", i), "score": int64(i)}},
			}}
			if _, err := handles[i].Commit(cur, b); err != nil {
				t.Fatalf("handle %d retry: %v", i, err)
			}
		}
	}
	if rows, _ := handles[0].Read(""); len(rows) != n {
		t.Fatalf("want %d rows after handle retries, got %d", n, len(rows))
	}
	t.Logf("多句柄冲突方重试后行数=%d，数据完整", n)
}

// ---------- worker 子进程 ----------

func workerMain(t *testing.T) {
	root := os.Getenv("TABLE_XPROC_ROOT")
	phase := os.Getenv("TABLE_XPROC_PHASE")
	parent := os.Getenv("TABLE_XPROC_PARENT")
	worker, _ := strconv.Atoi(os.Getenv("TABLE_XPROC_WORKER_ID"))

	store := NewStore(root)
	tbl, err := OpenTable(store, "orders")
	if err != nil {
		emit(workerResult{Worker: worker, Phase: phase, Error: err.Error(), Kind: "open"})
		return
	}
	// 短暂随机化起跑，放大真正的并发放大。
	time.Sleep(time.Duration(worker) * time.Millisecond)

	batchID := fmt.Sprintf("xproc-w%d", worker)
	row := Row{"id": fmt.Sprintf("u%d", worker), "name": fmt.Sprintf("user%d", worker), "score": int64(worker)}

	tryCommit := func(base string) (string, error) {
		return tbl.Commit(base, Batch{BatchID: batchID, Changes: []Change{{Upsert: &row}}})
	}

	switch phase {
	case "attempt":
		snapID, err := tryCommit(parent)
		emit(classify(worker, phase, snapID, err))
	case "retry":
		// 失败方循环基于最新 HEAD 重试，直到成功；首轮已成功的一方重放
		// 自己的批次会收到 ErrDuplicateBatch，保证不产生重复行。
		var snapID string
		var err error
		for attempt := 0; attempt < 100; attempt++ {
			cur, _ := tbl.Head()
			snapID, err = tryCommit(cur)
			if err == nil || errors.Is(err, ErrDuplicateBatch) {
				break
			}
			if !errors.Is(err, ErrCommitConflict) {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Millisecond)
		}
		emit(classify(worker, phase, snapID, err))
	}
}

func classify(worker int, phase, snapID string, err error) workerResult {
	r := workerResult{Worker: worker, Phase: phase, Snap: snapID}
	switch {
	case err == nil:
		r.OK = true
	case errors.Is(err, ErrCommitConflict):
		r.Kind = "conflict"
		r.Error = err.Error()
	case errors.Is(err, ErrDuplicateBatch):
		r.Kind = "duplicate"
		r.Error = err.Error()
	case errors.Is(err, ErrInvalidChange):
		r.Kind = "invalid"
		r.Error = err.Error()
	case errors.Is(err, ErrSchemaIncompatible):
		r.Kind = "incompatible"
		r.Error = err.Error()
	default:
		r.Kind = "other"
		r.Error = err.Error()
	}
	return r
}

func emit(r workerResult) {
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

// ---------- 父进程辅助 ----------

func buildWorkerBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "xproc.test")
	cmd := exec.Command("go", "test", "-c", "-o", bin, ".")
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build worker binary: %v\n%s", err, out)
	}
	return bin
}

func runWorkers(t *testing.T, bin, root, phase string, n int, parent string) []workerResult {
	t.Helper()
	var wg sync.WaitGroup
	resultsCh := make([]workerResult, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 同时起跑，保证真正并发
			cmd := exec.Command(bin, "-test.run", "TestCrossProcessConcurrentCommit", "-test.v")
			cmd.Env = append(os.Environ(),
				"TABLE_XPROC_WORKER=1",
				"TABLE_XPROC_ROOT="+root,
				"TABLE_XPROC_PHASE="+phase,
				"TABLE_XPROC_PARENT="+parent,
				"TABLE_XPROC_WORKER_ID="+strconv.Itoa(i),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				// worker 测试函数自身不调用 t.Fatal，正常退出码为 0。
				resultsCh[i] = workerResult{Worker: i, Phase: phase, Kind: "crash",
					Error: fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))}
				return
			}

			resultsCh[i] = parseWorkerOutput(i, phase, out)
		}(i)
	}
	close(start)
	wg.Wait()
	return resultsCh
}

func parseWorkerOutput(worker int, phase string, out []byte) workerResult {
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var last workerResult
	found := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var r workerResult
		if err := json.Unmarshal([]byte(line), &r); err == nil {
			last = r
			found = true
		}
	}
	if !found {
		return workerResult{Worker: worker, Phase: phase, Kind: "noresult",
			Error: strings.TrimSpace(string(out))}
	}
	return last
}

func summarize(rs []workerResult) (success, conflict, other int) {
	for _, r := range rs {
		switch r.Kind {
		case "":
			success++
		case "conflict":
			conflict++
		default:
			other++
		}
	}
	return
}

func countRows(t *testing.T, tbl *Table) int {
	t.Helper()
	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

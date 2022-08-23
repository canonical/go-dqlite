package benchmark

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
)

const (
	kvSchema = "CREATE TABLE IF NOT EXISTS model (key TEXT, value TEXT, UNIQUE(key))"
)

type Benchmark struct {
	app     *app.App
	db      *sql.DB
	dir     string
	options *options
	workers []*worker
}

func createWorkers(o *options) []*worker {
	workers := make([]*worker, o.nWorkers)
	for i := 0; i < o.nWorkers; i++ {
		switch o.workload {
		case kvWrite:
			workers[i] = newWorker(kvWriter, o)
		case kvReadWrite:
			workers[i] = newWorker(kvReaderWriter, o)
		}
	}
	return workers
}

func New(app *app.App, db *sql.DB, dir string, options ...Option) (bm *Benchmark, err error) {
	o := defaultOptions()
	for _, option := range options {
		option(o)
	}

	bm = &Benchmark{
		app:     app,
		db:      db,
		dir:     dir,
		options: o,
		workers: createWorkers(o),
	}

	return bm, nil
}

func (bm *Benchmark) runWorkload(ctx context.Context) {
	for _, worker := range bm.workers {
		go worker.run(ctx, bm.db)
	}
}

func (bm *Benchmark) kvSetup() error {
	_, err := bm.db.Exec(kvSchema)
	return err
}

func (bm *Benchmark) setup() error {
	switch bm.options.workload {

	default:
		return bm.kvSetup()
	}
}

func reportName(id int, work work) string {
	return fmt.Sprintf("%d-%s-%d", id, work, time.Now().Unix())
}

// Returns a map of filename to filecontent
func (bm *Benchmark) reportFiles() map[string]string {
	allReports := make(map[string]string)
	for i, worker := range bm.workers {
		reports := worker.report()
		for w, report := range reports {
			file := reportName(i, w)
			allReports[file] = fmt.Sprintf("%s", report)
		}
	}
	return allReports
}

func (bm *Benchmark) reportResults() error {
	dir := path.Join(bm.dir, "results")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create %v: %v", dir, err)
	}

	reports := bm.reportFiles()
	for filename, content := range reports {
		f, err := os.Create(path.Join(dir, filename))
		if err != nil {
			return fmt.Errorf("failed to create %v in %v: %v", filename, dir, err)
		}
		_, err = f.WriteString(content)
		if err != nil {
			return fmt.Errorf("failed to write %v in %v: %v", filename, dir, err)
		}
		f.Sync()
	}

	return nil
}

func (bm *Benchmark) nodeOnline(node *client.NodeInfo) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	cli, err := client.New(ctx, node.Address)
	if err != nil {
		return false
	}
	cli.Close()
	return true
}

func (bm *Benchmark) allNodesOnline(ctx context.Context, cancel context.CancelFunc) {
	for {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return
		}

		cli, err := bm.app.Client(ctx)
		if err != nil {
			continue
		}
		nodes, err := cli.Cluster(ctx)
		if err != nil {
			continue
		}
		cli.Close()

		n := 0
		for _, needed := range bm.options.cluster {
			for _, present := range nodes {
				if needed == present.Address && bm.nodeOnline(&present) {
					n += 1
				}
			}
		}
		if len(bm.options.cluster) == n {
			cancel()
			return
		}
	}
}

func (bm *Benchmark) waitForCluster(ch <-chan os.Signal) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(bm.options.clusterTimeout))
	defer cancel()

	go bm.allNodesOnline(ctx, cancel)

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			return fmt.Errorf("timed out waiting for cluster: %v", ctx.Err())
		}
		return nil
	case <-ch:
		return fmt.Errorf("benchmark stopped, signal received while waiting for cluster")
	}

}

func (bm *Benchmark) Run(ch <-chan os.Signal) error {
	if err := bm.setup(); err != nil {
		return err
	}

	if err := bm.waitForCluster(ch); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), bm.options.duration)
	defer cancel()

	bm.runWorkload(ctx)

	select {
	case <-ctx.Done():
		break
	case <-ch:
		cancel()
		break
	}

	if err := bm.reportResults(); err != nil {
		return err
	}
	fmt.Printf("Benchmark done. Results available here:\n%s\n", path.Join(bm.dir, "results"))
	return nil
}

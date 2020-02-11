package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
)

// match log formatting with Lmicroseconds set
const timeLayout = "2006/01/02 03:04:05.000000PM -0700"

func ts() string {
	return time.Now().Local().Format(timeLayout)
}

type tsWriter struct{}

func (t tsWriter) Write(in []byte) (int, error) {
	fmt.Println(ts(), string(in))
	return len(in), nil
}

func getStore(ctx context.Context, cluster []string) client.NodeStore {
	store := client.NewInmemNodeStore()
	infos := make([]client.NodeInfo, len(cluster))
	for i, address := range cluster {
		infos[i].ID = uint64(i + 1)
		infos[i].Address = address
	}
	store.Set(ctx, infos)
	return store
}

func getDB(ctx context.Context, dbName string, cluster []string, logger LogFunc) (*sql.DB, error) {
	store := getStore(ctx, cluster)
	if logger != nil {
		logger = client.DefaultLogFunc
	}
	logOpt := driver.WithLogFunc(logger)
	dbDriver, err := driver.New(store, logOpt)
	if err != nil {
		return nil, err
	}
	sql.Register("dqlite", dbDriver)

	db, err := sql.Open("dqlite", dbName)
	if err == nil {
		db.SetMaxOpenConns(1) // dqlite is single-threaded
	}
	return db, err
}

func prep(db *sql.DB) error {
	const drop = "drop table if exists simple"
	const create = "create table simple (id integer primary key, other integer, ts text)"
	if _, err := db.Exec(drop); err != nil {
		return err
	}
	_, err := db.Exec(create)
	return err
}

func hammer(count int, abort bool, logger LogFunc, dbName string, cluster []string) {
	db, err := getDB(context.Background(), dbName, cluster, logger)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("connected")
	defer db.Close()
	if err := prep(db); err != nil {
		log.Fatalln(err)
	}
	hammerTime(db, count, abort)
}

func hammerTime(db *sql.DB, count int, abort bool) {
	var now string
	var fails, good, last int

	started := time.Now().Local()
	fmt.Printf("%s starting\n", started.Format(timeLayout))
	for i := 0; ; i++ {
		if count > 0 && i >= count {
			break
		}
		const insert = "insert into simple (other, ts) values(%d,%q)"
		inserted := fmt.Sprintf(insert, last+1, ts())
		resp, err := db.Exec(inserted)
		if err != nil {
			fails++
			// first err (in a contiguous series?)
			if fails == 1 {
				// if so mark the starting point of the failure
				now = ts()

			}
			fmt.Printf("%s fails: %3d (%d/%d):%T %-30s\r", now, fails, i, count, err, err.Error())
			continue
		}
		if id, err := resp.LastInsertId(); err != nil {
			fmt.Printf("%s failed to get insert id: %+v\n", now, err)
			continue
		} else {
			last = int(id)
		}
		if fails > 0 {
			fmt.Printf("\n%s fixed: (%d/%d)\n", ts(), good, count)
			fails = 0
		}
		// As we're keeping track of every successful insert,
		// the generated id should always be in sync with
		// the known count of inserted records
		good++
		if last != good {
			// If there's a mismatch between expected and returned,
			// then we've encountered a double write, and this can
			// be shown by selecting records where other = good.
			fmt.Printf("%s offby: (%d/%d)\n", ts(), last, good)
			good = last // reset to avoid repeating same error
			if abort {
				break
			}
		}
		// visual reminder test is running
		if (good % 1000) == 0 {
			now = time.Now().Format(timeLayout)
			fmt.Printf("%s  good: (%d/%d)\n", ts(), good, count)
		}
	}
	delta := time.Now().Sub(started)
	total := good + fails
	if total > 0 {
		per := delta / time.Duration(total)
		fmt.Printf("\ncompleted %d/%d (%s/insert)\n", good, total, per)
	}
}

package main

import (
	"compress/bzip2"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	wikiparse "github.com/dustin/go-wikiparse"
	_ "github.com/mattn/go-sqlite3"
)

const pageTableSQL = `
CREATE TABLE page (
	id 	INTEGER PRIMARY KEY,
	title	VARCHAR NOT NULL UNIQUE,
	rev	INTEGER NOT NULL,
	ts	VARCHAR NOT NULL,
	text	VARCHAR NOT NULL,
	ptext	VARCHAR
)`

var dbFileName string
var diffDbFileName string
var wikiDumpFileName string

func tableExists(db *sql.DB, name string) bool {
	row := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name=?`, name)
	count := 0
	row.Scan(&count)
	return count != 0
}

func insertPage(tx *sql.Tx, page *wikiparse.Page) {
	_, err := tx.Exec(`INSERT INTO page(title, rev, ts, text) VALUES (?, ?, ?, ?)`, page.Title, page.Revisions[0].ID, page.Revisions[0].Timestamp, page.Revisions[0].Text)
	if err != nil {
		panic(err)
	}
}

func updatePage(tx *sql.Tx, page *wikiparse.Page) {
	_, err := tx.Exec(`UPDATE page SET rev=?, ts=?, text=? WHERE title=?`, page.Revisions[0].ID, page.Revisions[0].Timestamp, page.Revisions[0].Text, page.Title)
	if err != nil {
		panic(err)
	}
}

func processPage(tx, txDiff *sql.Tx, page *wikiparse.Page) {
	row := tx.QueryRow(`SELECT id, rev, ts FROM page WHERE title=?`, page.Title)
	var id, rev uint64
	var ts string
	err := row.Scan(&id, &rev, &ts)
	if err != nil {
		insertPage(tx, page)
		if txDiff != nil {
			insertPage(txDiff, page)
		}
		return
	}
	prev := page.Revisions[0]
	if prev.ID != rev {
		// log.Println("New revision for page", page.Title, "old:", rev, "new:", prev.ID)
		os.Stderr.Write([]byte{'*'})
		os.Stderr.Sync()

		updatePage(tx, page)
		if txDiff != nil {
			insertPage(txDiff, page)
		}
	}
}

func main() {
	timeStart := time.Now()
	flag.StringVar(&dbFileName, "db", "wiki.db", "SQLite database filename for the import")
	flag.StringVar(&wikiDumpFileName, "file", "", "Wikimedia dump XML file (possibly .bz2)")
	flag.StringVar(&diffDbFileName, "diff-db", "", "SQLite database filename for the diff (optional)")
	flag.Parse()

	if wikiDumpFileName == "" {
		fmt.Println("Must specify Wikimedia dump file")
		return
	}

	if _, err := os.Stat(wikiDumpFileName); os.IsNotExist(err) {
		fmt.Println("File not found:", wikiDumpFileName)
		return
	}

	db, err := sql.Open("sqlite3", dbFileName)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("PRAGMA cache_size=-204800")
	if err != nil {
		panic(err)
	}

	if !tableExists(db, "page") {
		_, err := db.Exec(pageTableSQL)
		if err != nil {
			panic(err)
		}
	}

	var dbDiff *sql.DB
	if diffDbFileName != "" {
		dbDiff, err = sql.Open("sqlite3", diffDbFileName)
		if err != nil {
			panic(err)
		}
		if !tableExists(dbDiff, "page") {
			_, err := dbDiff.Exec(pageTableSQL)
			if err != nil {
				panic(err)
			}
		}
	}

	var rd io.Reader
	if strings.HasSuffix(wikiDumpFileName, ".bz2") {
		f, err := os.Open(wikiDumpFileName)
		if err != nil {
			panic(err)
		}
		rd = bzip2.NewReader(f)
	} else {
		rd, err = os.Open(wikiDumpFileName)
		if err != nil {
			panic(err)
		}
	}

	p, err := wikiparse.NewParser(rd)
	if err != nil {
		panic(err)
	}

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	var txDiff *sql.Tx
	if dbDiff != nil {
		txDiff, err = dbDiff.Begin()
		if err != nil {
			panic(err)
		}
	}

	count := 0
	for err == nil {
		var page *wikiparse.Page
		page, err = p.Next()
		if err == nil {
			processPage(tx, txDiff, page)
		}
		count++
		if count%1000 == 0 {
			err = tx.Commit()
			if err != nil {
				panic(err)
			}
			tx, err = db.Begin()
			if err != nil {
				panic(err)
			}
			os.Stderr.Write([]byte{'.'})
			os.Stderr.Sync()
		}
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	if txDiff != nil {
		err = txDiff.Commit()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(count, "records")
	fmt.Println(time.Now().Sub(timeStart))
}

package main

import (
	"compress/bzip2"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
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

func processPage(tx *sql.Tx, page *wikiparse.Page) {
	row := tx.QueryRow(`SELECT id, rev, ts FROM page WHERE title=?`, page.Title)
	var id, rev uint64
	var ts time.Time
	err := row.Scan(&id, &rev, &ts)
	if err != nil {
		insertPage(tx, page)
		return
	}
	prev := page.Revisions[0]
	if prev.ID != id {
		log.Println("New revision for page", page.Title, "old:", rev, "new:", prev.ID)
		updatePage(tx, page)
	}
}

func main() {
	flag.StringVar(&dbFileName, "db", "wiki.db", "SQLite database filename")
	flag.StringVar(&wikiDumpFileName, "file", "", "Wikimedia dump XML file (possibly .bz2)")
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
		fmt.Println(err)
		return
	}

	if !tableExists(db, "page") {
		_, err := db.Exec(pageTableSQL)
		if err != nil {
			panic(err)
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

	count := 0
	for err == nil {
		var page *wikiparse.Page
		page, err = p.Next()
		if err == nil {
			processPage(tx, page)
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
}

package loader

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/eu-ovictor/b3-market-data/db"
	"github.com/schollz/progressbar/v3"
)

type loader struct {
	db db.DB
}

func (l loader) processFile(
	filePath string,
	batchSize int,
	pbar *progressbar.ProgressBar,
	wg *sync.WaitGroup,
) error {
	batch := []db.Trade{}

	r, err := newReader(filePath)
	if err != nil {
		return err
	}
	defer r.Close()

	// ignore header
	_, err = r.Read()
	if err != nil {
		return err
	}

	for {
		row, err := r.Read()

		if err != nil {
			if err == io.EOF {
				if err := l.db.InsertMany(batch); err != nil {
					return err
				}

				pbar.Add(len(batch))

				break
			}
			return err
		}

		trade, err := processRow(row)
		if err != nil {
			return err
		}

		batch = append(batch, trade)

		if len(batch) == batchSize {
			wg.Add(1)

			go func(wg *sync.WaitGroup, db db.DB, batch []db.Trade) {
				defer wg.Done()
				l.db.InsertMany(batch)
			}(wg, l.db, batch)

			pbar.Add(len(batch))

			batch = []db.Trade{}
		}
	}

	return nil
}

func Load(dir string, batchSize int, db db.DB) error {
	loader := loader{
		db: db,
	}

	pbar := progressbar.Default(-1, "rows inserted")
	defer pbar.Close()

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	q := make(chan error, len(files))

	for _, file := range files {
		filePath := filepath.Join(dir, file.Name())

		wg.Add(1)

		go func(filePath string, wg *sync.WaitGroup, q chan<- error) {
			defer wg.Done()

			err := loader.processFile(filePath, batchSize, pbar, wg)

			q <- err
		}(filePath, &wg, q)
	}

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(q)
	}(&wg)

	for m := range q {
		if m != nil {
			return m
		}
	}

	return nil
}

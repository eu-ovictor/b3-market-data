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
) error {
	var wg sync.WaitGroup

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
			}(&wg, l.db, batch)

			pbar.Add(len(batch))

			batch = []db.Trade{}
		}
	}

	wg.Wait()

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

	for _, file := range files {
		filePath := filepath.Join(dir, file.Name())

		if err := loader.processFile(filePath, batchSize, pbar); err != nil {
			return err
		}
	}
	return nil
}

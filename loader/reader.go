package loader

import (
	"archive/zip"
	"encoding/csv"
	"io"
	"path/filepath"
)

type tradeReader struct {
	zipFile *zip.ReadCloser
	file    io.ReadCloser
	reader  *csv.Reader
}

func (tr tradeReader) Close() error {
	tr.file.Close()
	tr.zipFile.Close()

	return nil
}

func newReader(filePath string) (tradeReader, error) {
	zf, err := zip.OpenReader(filePath)
	if err != nil {
		return tradeReader{}, err
	}

	var tradesFile *zip.File

	for _, f := range zf.File {
		if filepath.Ext(f.Name) == ".txt" {
			tradesFile = f
			break
		}
	}

	if tradesFile == nil {
		return tradeReader{}, nil
	}

	f, err := tradesFile.Open()
	if err != nil {
		return tradeReader{}, err
	}

	r := csv.NewReader(f)
	r.Comma = ';'

	return tradeReader{
		zipFile: zf,
		file:    f,
		reader:  r,
	}, nil
}

func (tr tradeReader) Read() ([]string, error) {
	return tr.reader.Read()
}

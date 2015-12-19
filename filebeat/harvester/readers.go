package harvester

type lineReader interface {
	Next() ([]byte, int, error)
}

// noEOLLineReader strips last EOL from input line reader (if present).
type noEOLLineReader struct {
	reader lineReader
}

func newNoEOLLineReader(r lineReader) *noEOLLineReader {
	return &noEOLLineReader{r}
}

func (n *noEOLLineReader) Next() ([]byte, int, error) {
	line, size, err := n.reader.Next()
	if err != nil {
		return line, size, err
	}

	return line[:len(line)-lineEndingChars(line)], size, err
}

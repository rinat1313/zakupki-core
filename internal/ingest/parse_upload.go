package ingest

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/xuri/excelize/v2"
)

// ParseUpload разбирает csv / xml / xlsx (и алиас .xmls) с двумя колонками: номер закупки, сайт.
func ParseUpload(filename string, r io.Reader) ([]struct{ Reg, Site string }, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("empty file")
	}
	ext := strings.ToLower(path.Ext(strings.TrimSpace(filename)))
	switch ext {
	case ".xlsx", ".xls", ".xmls":
		return ParseXLSX(bytes.NewReader(data))
	case ".xml":
		return ParseXML(data)
	case ".csv", ".txt", "":
		return ParseCSV(bytes.NewReader(data))
	default:
		trim := bytes.TrimSpace(data)
		if len(trim) >= 2 && trim[0] == 'P' && trim[1] == 'K' {
			return ParseXLSX(bytes.NewReader(data))
		}
		if len(trim) > 0 && (trim[0] == '<' || bytes.HasPrefix(trim, []byte{0xef, 0xbb, 0xbf, '<'})) {
			return ParseXML(data)
		}
		return ParseCSV(bytes.NewReader(data))
	}
}

func ParseXLSX(r io.Reader) ([]struct{ Reg, Site string }, error) {
	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("xlsx: %w", err)
	}
	defer f.Close()
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("xlsx: no sheets")
	}
	rows, err := f.GetRows(sheets[0])
	if err != nil {
		return nil, fmt.Errorf("xlsx: %w", err)
	}
	var out []struct{ Reg, Site string }
	first := true
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		cells := make([]string, len(row))
		for i, c := range row {
			cells[i] = strings.TrimSpace(c)
		}
		if first && looksHeader(strings.Join(cells, ";")) {
			first = false
			continue
		}
		first = false
		reg, site := parseCSVRow(cells)
		if reg == "" && site == "" {
			continue
		}
		if reg == "" {
			reg = guessRegFromURL(site)
		}
		if site == "" {
			site = "https://zakupki.gov.ru"
		}
		if reg == "" {
			continue
		}
		out = append(out, struct{ Reg, Site string }{reg, site})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty xlsx")
	}
	return out, nil
}

type xmlElem struct {
	name  string
	attrs map[string]string
	text  string
	kids  []xmlElem
}

func ParseXML(data []byte) ([]struct{ Reg, Site string }, error) {
	root, err := parseXMLTree(data)
	if err != nil {
		return nil, err
	}
	var out []struct{ Reg, Site string }
	var walk func(e xmlElem)
	walk = func(e xmlElem) {
		if isXMLRowTag(e.name) {
			reg, site := extractXMLPair(e)
			if reg != "" || site != "" {
				if reg == "" {
					reg = guessRegFromURL(site)
				}
				if site == "" {
					site = "https://zakupki.gov.ru"
				}
				if reg != "" {
					out = append(out, struct{ Reg, Site string }{reg, site})
				}
				return
			}
		}
		for _, ch := range e.kids {
			walk(ch)
		}
	}
	walk(root)
	if len(out) == 0 {
		leaves := collectXMLLeaves(root)
		for i := 0; i+1 < len(leaves); i += 2 {
			reg, site := parseCSVRow([]string{leaves[i], leaves[i+1]})
			if reg == "" {
				reg = guessRegFromURL(site)
			}
			if site == "" {
				site = "https://zakupki.gov.ru"
			}
			if reg == "" {
				continue
			}
			out = append(out, struct{ Reg, Site string }{reg, site})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty xml")
	}
	return out, nil
}

func parseXMLTree(data []byte) (xmlElem, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	dec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) { return input, nil }
	var stack []xmlElem
	var root xmlElem
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return xmlElem{}, fmt.Errorf("xml: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			el := xmlElem{name: strings.ToLower(t.Name.Local), attrs: map[string]string{}}
			for _, a := range t.Attr {
				el.attrs[strings.ToLower(a.Name.Local)] = strings.TrimSpace(a.Value)
			}
			stack = append(stack, el)
		case xml.EndElement:
			if len(stack) == 0 {
				continue
			}
			el := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			el.text = strings.TrimSpace(el.text)
			if len(stack) == 0 {
				root = el
			} else {
				parent := &stack[len(stack)-1]
				parent.kids = append(parent.kids, el)
			}
		case xml.CharData:
			if len(stack) == 0 {
				continue
			}
			stack[len(stack)-1].text += string(t)
		}
	}
	if root.name == "" && len(root.kids) == 0 {
		return xmlElem{}, fmt.Errorf("xml: empty document")
	}
	return root, nil
}

func isXMLRowTag(name string) bool {
	switch strings.ToLower(name) {
	case "row", "item", "entry", "record", "tender", "zakupka", "notice", "line":
		return true
	default:
		return false
	}
}

func isXMLRegField(name string) bool {
	n := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	switch n {
	case "reg", "regnumber", "reg_number", "number", "номер", "номерзакупки", "номер_закупки",
		"reestrnumber", "reestr_number", "purchase_number", "id":
		return true
	default:
		return strings.Contains(n, "reg") || strings.Contains(n, "номер")
	}
}

func isXMLSiteField(name string) bool {
	n := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	switch n {
	case "site", "url", "link", "href", "сайт", "площадка", "source", "source_site", "sourcesite":
		return true
	default:
		return strings.Contains(n, "site") || strings.Contains(n, "сайт") || strings.Contains(n, "url")
	}
}

func extractXMLPair(e xmlElem) (reg, site string) {
	for k, v := range e.attrs {
		if v == "" {
			continue
		}
		if isXMLRegField(k) && reg == "" {
			reg = v
		} else if isXMLSiteField(k) && site == "" {
			site = v
		}
	}
	var texts []string
	for _, ch := range e.kids {
		val := strings.TrimSpace(ch.text)
		if val == "" && len(ch.kids) == 0 {
			continue
		}
		if val == "" {
			continue
		}
		if isXMLRegField(ch.name) && reg == "" {
			reg = val
			continue
		}
		if isXMLSiteField(ch.name) && site == "" {
			site = val
			continue
		}
		texts = append(texts, val)
	}
	if reg == "" || site == "" {
		for _, t := range texts {
			if looksURL(t) && site == "" {
				site = t
			} else if !looksURL(t) && reg == "" {
				reg = t
			} else if site == "" {
				site = t
			} else if reg == "" {
				reg = t
			}
		}
	}
	return reg, site
}

func collectXMLLeaves(e xmlElem) []string {
	var out []string
	var walk func(xmlElem)
	walk = func(e xmlElem) {
		if len(e.kids) == 0 {
			v := strings.TrimSpace(e.text)
			if v != "" {
				out = append(out, v)
			}
			return
		}
		for _, ch := range e.kids {
			walk(ch)
		}
	}
	walk(e)
	return out
}

package ingest

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseCSVBasic(t *testing.T) {
	in := "номер;сайт\n0373100000123000001;https://zakupki.gov.ru\n"
	items, err := ParseUpload("list.csv", strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Reg != "0373100000123000001" {
		t.Fatalf("%+v", items)
	}
}

func TestParseXMLRows(t *testing.T) {
	in := `<?xml version="1.0"?>
<root>
  <row><номер>111</номер><сайт>https://zakupki.gov.ru</сайт></row>
  <item reg="222" site="https://mos.ru"/>
</root>`
	items, err := ParseUpload("list.xml", strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 got %+v", items)
	}
	if items[0].Reg != "111" || items[1].Reg != "222" {
		t.Fatalf("%+v", items)
	}
}

func TestParseUploadSniffXML(t *testing.T) {
	in := []byte(`<rows><row><reg>333</reg><url>https://tektorg.ru</url></row></rows>`)
	items, err := ParseUpload("unknown.bin", bytes.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Reg != "333" {
		t.Fatalf("%+v", items)
	}
}

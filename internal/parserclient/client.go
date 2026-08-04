package parserclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		HTTPClient: &http.Client{
			Timeout: 6 * time.Minute,
		},
	}
}

func (c *Client) Enabled() bool {
	return c != nil && c.BaseURL != ""
}

type FetchRequest struct {
	RegNumber  string `json:"reg_number"`
	SourceSite string `json:"source_site"`
}

type Doc struct {
	UID           string `json:"UID"`
	Filename      string `json:"Filename"`
	SourceURL     string `json:"SourceURL"`
	GroupTitle    string `json:"GroupTitle"`
	Edition       string `json:"Edition"`
	ProcessStatus string `json:"ProcessStatus"`
	TextContent   string `json:"TextContent"`
	ProcessError  string `json:"ProcessError"`
	ContentHash   string `json:"ContentHash"`
}

type Customer struct {
	INN              string `json:"inn"`
	KPP              string `json:"kpp"`
	OGRN             string `json:"ogrn"`
	FullName         string `json:"full_name"`
	Address          string `json:"address"`
	Email            string `json:"email"`
	Phone            string `json:"phone"`
	ContactPerson    string `json:"contact_person"`
	OrganizationCode string `json:"organization_code"`
	AgencyID         string `json:"agency_id"`
}

type Result struct {
	RegNumber      string          `json:"RegNumber"`
	Law            string          `json:"Law"`
	ObjectName     string          `json:"ObjectName"`
	Status         string          `json:"Status"`
	NMCK           *float64        `json:"NMCK"`
	Currency       string          `json:"Currency"`
	PublishedAt    *time.Time      `json:"PublishedAt"`
	UpdatedOnSite  *time.Time      `json:"UpdatedOnSite"`
	ApplicationEnd *time.Time      `json:"ApplicationEnd"`
	Customer       Customer        `json:"Customer"`
	Customer223    *Customer       `json:"Customer223"`
	Payload        json.RawMessage `json:"Payload"`
	Documents      []Doc           `json:"Documents"`
}

type FetchResponse struct {
	RegNumber  string  `json:"reg_number"`
	SourceSite string  `json:"source_site"`
	SourceUsed string  `json:"source_used"`
	Failed     bool    `json:"failed"`
	Message    string  `json:"message"`
	Result     *Result `json:"result"`
}

func (c *Client) Fetch(ctx context.Context, reg, site string) (*FetchResponse, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("PARSER_URL not set")
	}
	body, _ := json.Marshal(FetchRequest{RegNumber: reg, SourceSite: site})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/fetch", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("parser HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out FetchResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

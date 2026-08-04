package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Category struct {
	ID               uuid.UUID  `json:"id"`
	Slug             string     `json:"slug"`
	Title            string     `json:"title"`
	ActiveAIConfigID *uuid.UUID `json:"active_ai_config_id,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

type Customer struct {
	ID               uuid.UUID       `json:"id"`
	INN              string          `json:"inn"`
	KPP              string          `json:"kpp"`
	OGRN             string          `json:"ogrn"`
	FullName         string          `json:"full_name"`
	ShortName        string          `json:"short_name"`
	Address          string          `json:"address"`
	Email            string          `json:"email"`
	Phone            string          `json:"phone"`
	ContactPerson    string          `json:"contact_person"`
	OrganizationCode string          `json:"organization_code"`
	AgencyID         string          `json:"agency_id"`
	Payload          json.RawMessage `json:"payload"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

type Tender struct {
	ID              uuid.UUID       `json:"id"`
	RegNumber       string          `json:"reg_number"`
	SourceSite      string          `json:"source_site"`
	Law             string          `json:"law"`
	CustomerID      *uuid.UUID      `json:"customer_id,omitempty"`
	ObjectName      string          `json:"object_name"`
	Status          string          `json:"status"`
	NMCK            *float64        `json:"nmck,omitempty"`
	Currency        string          `json:"currency"`
	PublishedAt     *time.Time      `json:"published_at,omitempty"`
	UpdatedOnSite   *time.Time      `json:"updated_on_site,omitempty"`
	ApplicationEnd  *time.Time      `json:"application_end,omitempty"`
	AnalysisStatus  string          `json:"analysis_status"`
	Payload         json.RawMessage `json:"payload"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	CategorySlugs   []string        `json:"category_slugs,omitempty"`

	// Прогресс для UI (заполняется в ListTenders).
	DocsTotal       int      `json:"docs_total"`
	DocsProcessed   int      `json:"docs_processed"`
	DocsUnprocessed int      `json:"docs_unprocessed"`
	DocsWithText    int      `json:"docs_with_text"`
	DocsErrors      int      `json:"docs_errors"`
	CollectPct      int      `json:"collect_pct"`
	CollectOK       *bool    `json:"collect_ok,omitempty"` // nil=в процессе, true/false=итог
	AIPct           int      `json:"ai_pct"`
	AIOK            *bool    `json:"ai_ok,omitempty"`
	IngestStatus    string   `json:"ingest_status,omitempty"`
	Recommendation  string   `json:"recommendation,omitempty"`
	AssessScore     *float64 `json:"assess_score,omitempty"`
	ReadyForAI      bool     `json:"ready_for_ai"`
	CardTone        string   `json:"card_tone,omitempty"` // good|bad|pending|neutral
	StoredCollectPct int     `json:"-"`
	StoredAIPct      int     `json:"-"`
}

type Document struct {
	ID            uuid.UUID `json:"id"`
	TenderID      uuid.UUID `json:"tender_id"`
	UID           string    `json:"uid"`
	Filename      string    `json:"filename"`
	SourceURL     string    `json:"source_url"`
	GroupTitle    string    `json:"group_title"`
	Edition       string    `json:"edition"`
	ProcessStatus string    `json:"process_status"`
	TextContent   *string   `json:"text_content,omitempty"`
	ProcessError  string    `json:"process_error"`
	ContentHash   string    `json:"content_hash"`
	Removed       bool      `json:"removed"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Assessment struct {
	TenderID  uuid.UUID       `json:"tender_id"`
	Summary   string          `json:"summary"`
	Score     *float64        `json:"score,omitempty"`
	Details   json.RawMessage `json:"details"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type IngestJob struct {
	ID          uuid.UUID `json:"id"`
	CategoryID  uuid.UUID `json:"category_id"`
	CategorySlug string   `json:"category_slug,omitempty"`
	CategoryTitle string  `json:"category_title,omitempty"`
	SourceName  string    `json:"source_name"`
	Status      string    `json:"status"`
	TotalItems  int       `json:"total_items"`
	DoneItems   int       `json:"done_items"`
	ErrorItems  int       `json:"error_items"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type IngestItem struct {
	ID         uuid.UUID  `json:"id"`
	JobID      uuid.UUID  `json:"job_id"`
	RegNumber  string     `json:"reg_number"`
	SourceSite string     `json:"source_site"`
	Status     string     `json:"status"`
	Error      string     `json:"error"`
	TenderID   *uuid.UUID `json:"tender_id,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type Store struct {
	Pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

func slugify(title string) string {
	s := strings.ToLower(strings.TrimSpace(title))
	repl := strings.NewReplacer(
		" ", "-", "_", "-", "/", "-", "\\", "-", ",", "", ".", "",
		"«", "", "»", "", "\"", "", "'", "",
	)
	s = repl.Replace(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || (r >= 'а' && r <= 'я') || r == 'ё' {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" {
		out = "cat-" + uuid.NewString()[:8]
	}
	return out
}

func (s *Store) ListCategories(ctx context.Context) ([]Category, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, slug, title, active_ai_config_id, created_at FROM categories ORDER BY title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Category
	for rows.Next() {
		var c Category
		if err := rows.Scan(&c.ID, &c.Slug, &c.Title, &c.ActiveAIConfigID, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) GetCategoryBySlug(ctx context.Context, slug string) (*Category, error) {
	var c Category
	err := s.Pool.QueryRow(ctx, `SELECT id, slug, title, active_ai_config_id, created_at FROM categories WHERE slug=$1`, slug).
		Scan(&c.ID, &c.Slug, &c.Title, &c.ActiveAIConfigID, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) CreateCategory(ctx context.Context, title, slug string) (*Category, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, fmt.Errorf("title required")
	}
	if slug == "" {
		slug = slugify(title)
	}
	var c Category
	err := s.Pool.QueryRow(ctx,
		`INSERT INTO categories(slug, title) VALUES($1,$2)
		 ON CONFLICT (slug) DO UPDATE SET title=EXCLUDED.title
		 RETURNING id, slug, title, active_ai_config_id, created_at`, slug, title).
		Scan(&c.ID, &c.Slug, &c.Title, &c.ActiveAIConfigID, &c.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) UpsertCustomer(ctx context.Context, c Customer) (*Customer, error) {
	if c.Payload == nil {
		c.Payload = json.RawMessage(`{}`)
	}
	var out Customer
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO customers(inn,kpp,ogrn,full_name,short_name,address,email,phone,contact_person,organization_code,agency_id,payload,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())
		ON CONFLICT (inn,kpp) DO UPDATE SET
		  ogrn=EXCLUDED.ogrn,
		  full_name=COALESCE(NULLIF(EXCLUDED.full_name,''), customers.full_name),
		  short_name=COALESCE(NULLIF(EXCLUDED.short_name,''), customers.short_name),
		  address=COALESCE(NULLIF(EXCLUDED.address,''), customers.address),
		  email=COALESCE(NULLIF(EXCLUDED.email,''), customers.email),
		  phone=COALESCE(NULLIF(EXCLUDED.phone,''), customers.phone),
		  contact_person=COALESCE(NULLIF(EXCLUDED.contact_person,''), customers.contact_person),
		  organization_code=COALESCE(NULLIF(EXCLUDED.organization_code,''), customers.organization_code),
		  agency_id=COALESCE(NULLIF(EXCLUDED.agency_id,''), customers.agency_id),
		  payload=EXCLUDED.payload,
		  updated_at=now()
		RETURNING id,inn,kpp,ogrn,full_name,short_name,address,email,phone,contact_person,organization_code,agency_id,payload,created_at,updated_at`,
		c.INN, c.KPP, c.OGRN, c.FullName, c.ShortName, c.Address, c.Email, c.Phone, c.ContactPerson, c.OrganizationCode, c.AgencyID, c.Payload,
	).Scan(&out.ID, &out.INN, &out.KPP, &out.OGRN, &out.FullName, &out.ShortName, &out.Address, &out.Email, &out.Phone, &out.ContactPerson, &out.OrganizationCode, &out.AgencyID, &out.Payload, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) ListCustomers(ctx context.Context, q string, limit int) ([]Customer, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q = strings.TrimSpace(q)
	rows, err := s.Pool.Query(ctx, `
		SELECT id,inn,kpp,ogrn,full_name,short_name,address,email,phone,contact_person,organization_code,agency_id,payload,created_at,updated_at
		FROM customers
		WHERE ($1='' OR inn ILIKE '%'||$1||'%' OR full_name ILIKE '%'||$1||'%' OR kpp ILIKE '%'||$1||'%')
		ORDER BY updated_at DESC LIMIT $2`, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Customer
	for rows.Next() {
		var c Customer
		if err := rows.Scan(&c.ID, &c.INN, &c.KPP, &c.OGRN, &c.FullName, &c.ShortName, &c.Address, &c.Email, &c.Phone, &c.ContactPerson, &c.OrganizationCode, &c.AgencyID, &c.Payload, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) GetCustomer(ctx context.Context, id uuid.UUID) (*Customer, error) {
	var c Customer
	err := s.Pool.QueryRow(ctx, `
		SELECT id,inn,kpp,ogrn,full_name,short_name,address,email,phone,contact_person,organization_code,agency_id,payload,created_at,updated_at
		FROM customers WHERE id=$1`, id).
		Scan(&c.ID, &c.INN, &c.KPP, &c.OGRN, &c.FullName, &c.ShortName, &c.Address, &c.Email, &c.Phone, &c.ContactPerson, &c.OrganizationCode, &c.AgencyID, &c.Payload, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) UpdateCustomer(ctx context.Context, id uuid.UUID, c Customer) (*Customer, error) {
	if c.Payload == nil {
		c.Payload = json.RawMessage(`{}`)
	}
	var out Customer
	err := s.Pool.QueryRow(ctx, `
		UPDATE customers SET inn=$2,kpp=$3,ogrn=$4,full_name=$5,short_name=$6,address=$7,email=$8,phone=$9,
		contact_person=$10,organization_code=$11,agency_id=$12,payload=$13,updated_at=now()
		WHERE id=$1
		RETURNING id,inn,kpp,ogrn,full_name,short_name,address,email,phone,contact_person,organization_code,agency_id,payload,created_at,updated_at`,
		id, c.INN, c.KPP, c.OGRN, c.FullName, c.ShortName, c.Address, c.Email, c.Phone, c.ContactPerson, c.OrganizationCode, c.AgencyID, c.Payload,
	).Scan(&out.ID, &out.INN, &out.KPP, &out.OGRN, &out.FullName, &out.ShortName, &out.Address, &out.Email, &out.Phone, &out.ContactPerson, &out.OrganizationCode, &out.AgencyID, &out.Payload, &out.CreatedAt, &out.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) DeleteCustomer(ctx context.Context, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM customers WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

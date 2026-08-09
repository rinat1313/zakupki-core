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
	// SearchConfigID — id настройки поиска (zakupki-search profile id).
	SearchConfigID *string `json:"search_config_id,omitempty"`
	// SearchProfileID — алиас search_config_id для контракта zakupki-search / UI.
	SearchProfileID *string `json:"search_profile_id,omitempty"`
	// AutoAI — включить автосбор AI-анализа для тендеров этого поисковика.
	AutoAI               bool       `json:"auto_ai"`
	SyncedConfigVersion  int64      `json:"synced_config_version"`
	ActiveAIConfigID     *uuid.UUID `json:"active_ai_config_id,omitempty"`
	TendersCount         int        `json:"tenders_count,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
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
	// Retained — закупка сохранена вне пула поисковика (интересная / в работе / AI).
	// При смене настроек поиска и sync пула такая запись не удаляется из БД.
	Retained     bool       `json:"retained"`
	RetainedAt   *time.Time `json:"retained_at,omitempty"`
	RetainReason string     `json:"retain_reason,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	CategorySlugs []string  `json:"category_slugs,omitempty"`

	// Прогресс для UI (заполняется в ListTenders).
	DocsTotal        int      `json:"docs_total"`
	DocsProcessed    int      `json:"docs_processed"`
	DocsUnprocessed  int      `json:"docs_unprocessed"`
	DocsWithText     int      `json:"docs_with_text"`
	DocsErrors       int      `json:"docs_errors"`
	CollectPct       int      `json:"collect_pct"`
	CollectOK        *bool    `json:"collect_ok,omitempty"` // nil=в процессе, true/false=итог
	AIPct            int      `json:"ai_pct"`
	AIOK             *bool    `json:"ai_ok,omitempty"`
	IngestStatus     string   `json:"ingest_status,omitempty"`
	Recommendation   string   `json:"recommendation,omitempty"`
	AssessScore      *float64 `json:"assess_score,omitempty"`
	AssessSummary    string   `json:"assess_summary,omitempty"`
	ReadyForAI       bool     `json:"ready_for_ai"`
	CardTone         string   `json:"card_tone,omitempty"` // good|bad|pending|neutral
	InSearchPool     bool     `json:"in_search_pool"` // есть связь с категорией поисковика
	StoredCollectPct int      `json:"-"`
	StoredAIPct      int      `json:"-"`
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

func decorateCategory(c *Category) {
	if c == nil {
		return
	}
	c.SearchProfileID = c.SearchConfigID
}

func scanCategory(row pgx.Row) (*Category, error) {
	var c Category
	err := row.Scan(&c.ID, &c.Slug, &c.Title, &c.SearchConfigID, &c.AutoAI, &c.SyncedConfigVersion, &c.ActiveAIConfigID, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	decorateCategory(&c)
	return &c, nil
}

const categoryCols = `id, slug, title, search_config_id, COALESCE(auto_ai,false), COALESCE(synced_config_version,0), active_ai_config_id, created_at`

func (s *Store) ListCategories(ctx context.Context) ([]Category, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT c.id, c.slug, c.title, c.search_config_id, COALESCE(c.auto_ai,false), COALESCE(c.synced_config_version,0),
		       c.active_ai_config_id, c.created_at,
		       COALESCE((SELECT count(*) FROM tender_categories tc WHERE tc.category_id=c.id),0)
		FROM categories c
		ORDER BY c.title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Category
	for rows.Next() {
		var c Category
		if err := rows.Scan(&c.ID, &c.Slug, &c.Title, &c.SearchConfigID, &c.AutoAI, &c.SyncedConfigVersion,
			&c.ActiveAIConfigID, &c.CreatedAt, &c.TendersCount); err != nil {
			return nil, err
		}
		decorateCategory(&c)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) GetCategoryBySlug(ctx context.Context, slug string) (*Category, error) {
	return scanCategory(s.Pool.QueryRow(ctx, `SELECT `+categoryCols+` FROM categories WHERE slug=$1`, slug))
}

func (s *Store) GetCategoryBySearchConfigID(ctx context.Context, searchConfigID string) (*Category, error) {
	searchConfigID = strings.TrimSpace(searchConfigID)
	if searchConfigID == "" {
		return nil, ErrNotFound
	}
	return scanCategory(s.Pool.QueryRow(ctx,
		`SELECT `+categoryCols+` FROM categories WHERE search_config_id=$1`, searchConfigID))
}

func (s *Store) CreateCategory(ctx context.Context, title, slug string) (*Category, error) {
	return s.CreateCategoryWithSearchConfig(ctx, title, slug, "")
}

func (s *Store) CreateCategoryWithSearchConfig(ctx context.Context, title, slug, searchConfigID string) (*Category, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, fmt.Errorf("title required")
	}
	if slug == "" {
		slug = slugify(title)
	}
	searchConfigID = strings.TrimSpace(searchConfigID)
	var searchPtr any
	if searchConfigID != "" {
		searchPtr = searchConfigID
	}
	return scanCategory(s.Pool.QueryRow(ctx,
		`INSERT INTO categories(slug, title, search_config_id) VALUES($1,$2,$3)
		 ON CONFLICT (slug) DO UPDATE SET
		   title=EXCLUDED.title,
		   search_config_id=COALESCE(EXCLUDED.search_config_id, categories.search_config_id)
		 RETURNING `+categoryCols, slug, title, searchPtr))
}

// UpdateCategory patches title/slug/search_config_id. Nil pointers mean "leave unchanged".
// Empty searchConfigID string clears the binding.
func (s *Store) UpdateCategory(ctx context.Context, slug string, title, newSlug, searchConfigID *string) (*Category, error) {
	cur, err := s.GetCategoryBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	if title != nil {
		t := strings.TrimSpace(*title)
		if t == "" {
			return nil, fmt.Errorf("title required")
		}
		cur.Title = t
	}
	if newSlug != nil {
		ns := strings.TrimSpace(*newSlug)
		if ns == "" {
			return nil, fmt.Errorf("slug required")
		}
		cur.Slug = ns
	}
	var searchPtr any
	if searchConfigID != nil {
		v := strings.TrimSpace(*searchConfigID)
		if v == "" {
			cur.SearchConfigID = nil
			searchPtr = nil
		} else {
			cur.SearchConfigID = &v
			searchPtr = v
		}
	} else if cur.SearchConfigID != nil {
		searchPtr = *cur.SearchConfigID
	}
	return scanCategory(s.Pool.QueryRow(ctx, `
		UPDATE categories SET slug=$2, title=$3, search_config_id=$4
		WHERE id=$1
		RETURNING `+categoryCols, cur.ID, cur.Slug, cur.Title, searchPtr))
}

func (s *Store) SetCategoryAutoAI(ctx context.Context, categoryID uuid.UUID, enabled bool) (*Category, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE categories SET auto_ai=$2 WHERE id=$1`, categoryID, enabled)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	return scanCategory(s.Pool.QueryRow(ctx, `SELECT `+categoryCols+` FROM categories WHERE id=$1`, categoryID))
}

func (s *Store) AnyCategoryAutoAI(ctx context.Context) (bool, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM categories WHERE auto_ai`).Scan(&n)
	return n > 0, err
}

func (s *Store) SetSyncedConfigVersion(ctx context.Context, categoryID uuid.UUID, version int64) error {
	_, err := s.Pool.Exec(ctx, `UPDATE categories SET synced_config_version=$2 WHERE id=$1`, categoryID, version)
	return err
}

func (s *Store) DeleteCategory(ctx context.Context, slug string) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM categories WHERE slug=$1`, slug)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
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

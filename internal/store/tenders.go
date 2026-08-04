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
)

func (s *Store) ListTenders(ctx context.Context, categorySlug, q, status string, limit int) ([]Tender, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT t.id,t.reg_number,t.source_site,t.law,t.customer_id,t.object_name,t.status,t.nmck,t.currency,
		       t.published_at,t.updated_on_site,t.application_end,t.analysis_status,t.payload,t.created_at,t.updated_at,
		       COALESCE(array_agg(c.slug) FILTER (WHERE c.slug IS NOT NULL), '{}')
		FROM tenders t
		LEFT JOIN tender_categories tc ON tc.tender_id=t.id
		LEFT JOIN categories c ON c.id=tc.category_id
		WHERE ($1='' OR c.slug=$1)
		  AND ($2='' OR t.reg_number ILIKE '%'||$2||'%' OR t.object_name ILIKE '%'||$2||'%')
		  AND ($3='' OR t.analysis_status::text=$3)
		GROUP BY t.id
		ORDER BY t.application_end NULLS LAST, t.updated_at DESC
		LIMIT $4`, categorySlug, strings.TrimSpace(q), strings.TrimSpace(status), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tender
	for rows.Next() {
		var t Tender
		if err := rows.Scan(&t.ID, &t.RegNumber, &t.SourceSite, &t.Law, &t.CustomerID, &t.ObjectName, &t.Status, &t.NMCK, &t.Currency,
			&t.PublishedAt, &t.UpdatedOnSite, &t.ApplicationEnd, &t.AnalysisStatus, &t.Payload, &t.CreatedAt, &t.UpdatedAt, &t.CategorySlugs); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) GetTender(ctx context.Context, id uuid.UUID) (*Tender, error) {
	var t Tender
	err := s.Pool.QueryRow(ctx, `
		SELECT t.id,t.reg_number,t.source_site,t.law,t.customer_id,t.object_name,t.status,t.nmck,t.currency,
		       t.published_at,t.updated_on_site,t.application_end,t.analysis_status,t.payload,t.created_at,t.updated_at,
		       COALESCE(array_agg(c.slug) FILTER (WHERE c.slug IS NOT NULL), '{}')
		FROM tenders t
		LEFT JOIN tender_categories tc ON tc.tender_id=t.id
		LEFT JOIN categories c ON c.id=tc.category_id
		WHERE t.id=$1
		GROUP BY t.id`, id).
		Scan(&t.ID, &t.RegNumber, &t.SourceSite, &t.Law, &t.CustomerID, &t.ObjectName, &t.Status, &t.NMCK, &t.Currency,
			&t.PublishedAt, &t.UpdatedOnSite, &t.ApplicationEnd, &t.AnalysisStatus, &t.Payload, &t.CreatedAt, &t.UpdatedAt, &t.CategorySlugs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Store) FindTenderByKey(ctx context.Context, reg, site string) (*Tender, error) {
	var t Tender
	err := s.Pool.QueryRow(ctx, `
		SELECT id,reg_number,source_site,law,customer_id,object_name,status,nmck,currency,
		       published_at,updated_on_site,application_end,analysis_status,payload,created_at,updated_at
		FROM tenders WHERE reg_number=$1 AND source_site=$2`, reg, site).
		Scan(&t.ID, &t.RegNumber, &t.SourceSite, &t.Law, &t.CustomerID, &t.ObjectName, &t.Status, &t.NMCK, &t.Currency,
			&t.PublishedAt, &t.UpdatedOnSite, &t.ApplicationEnd, &t.AnalysisStatus, &t.Payload, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

type TenderUpsertInput struct {
	RegNumber      string
	SourceSite     string
	Law            string
	CustomerID     *uuid.UUID
	ObjectName     string
	Status         string
	NMCK           *float64
	Currency       string
	PublishedAt    *time.Time
	UpdatedOnSite  *time.Time
	ApplicationEnd *time.Time
	Payload        json.RawMessage
	CategoryID     uuid.UUID
}

func (s *Store) UpsertTender(ctx context.Context, in TenderUpsertInput) (*Tender, bool, error) {
	if in.Payload == nil {
		in.Payload = json.RawMessage(`{}`)
	}
	if in.Currency == "" {
		in.Currency = "RUB"
	}
	existing, err := s.FindTenderByKey(ctx, in.RegNumber, in.SourceSite)
	created := false
	if errors.Is(err, ErrNotFound) {
		created = true
	} else if err != nil {
		return nil, false, err
	}

	var t Tender
	err = s.Pool.QueryRow(ctx, `
		INSERT INTO tenders(reg_number,source_site,law,customer_id,object_name,status,nmck,currency,
		  published_at,updated_on_site,application_end,payload,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())
		ON CONFLICT (reg_number, source_site) DO UPDATE SET
		  law=COALESCE(NULLIF(EXCLUDED.law,''), tenders.law),
		  customer_id=COALESCE(EXCLUDED.customer_id, tenders.customer_id),
		  object_name=COALESCE(NULLIF(EXCLUDED.object_name,''), tenders.object_name),
		  status=COALESCE(NULLIF(EXCLUDED.status,''), tenders.status),
		  nmck=COALESCE(EXCLUDED.nmck, tenders.nmck),
		  currency=COALESCE(NULLIF(EXCLUDED.currency,''), tenders.currency),
		  published_at=COALESCE(EXCLUDED.published_at, tenders.published_at),
		  updated_on_site=COALESCE(EXCLUDED.updated_on_site, tenders.updated_on_site),
		  application_end=COALESCE(EXCLUDED.application_end, tenders.application_end),
		  payload=EXCLUDED.payload,
		  updated_at=now()
		RETURNING id,reg_number,source_site,law,customer_id,object_name,status,nmck,currency,
		  published_at,updated_on_site,application_end,analysis_status,payload,created_at,updated_at`,
		in.RegNumber, in.SourceSite, in.Law, in.CustomerID, in.ObjectName, in.Status, in.NMCK, in.Currency,
		in.PublishedAt, in.UpdatedOnSite, in.ApplicationEnd, in.Payload,
	).Scan(&t.ID, &t.RegNumber, &t.SourceSite, &t.Law, &t.CustomerID, &t.ObjectName, &t.Status, &t.NMCK, &t.Currency,
		&t.PublishedAt, &t.UpdatedOnSite, &t.ApplicationEnd, &t.AnalysisStatus, &t.Payload, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, false, err
	}

	_, _ = s.Pool.Exec(ctx, `
		INSERT INTO tender_categories(tender_id, category_id) VALUES($1,$2)
		ON CONFLICT DO NOTHING`, t.ID, in.CategoryID)

	if created {
		_, _ = s.AddEvent(ctx, t.ID, "created", "тендер создан", nil)
	} else if existing != nil {
		s.diffAndLog(ctx, existing, &t)
	}
	return &t, created, nil
}

func (s *Store) diffAndLog(ctx context.Context, oldT, newT *Tender) {
	if (oldT.NMCK == nil && newT.NMCK != nil) || (oldT.NMCK != nil && newT.NMCK != nil && *oldT.NMCK != *newT.NMCK) {
		_, _ = s.AddEvent(ctx, newT.ID, "nmck_changed", fmt.Sprintf("НМЦК: %v → %v", oldT.NMCK, newT.NMCK), map[string]any{"old": oldT.NMCK, "new": newT.NMCK})
	}
	if !timePtrEqual(oldT.ApplicationEnd, newT.ApplicationEnd) {
		_, _ = s.AddEvent(ctx, newT.ID, "application_end_changed", "дата окончания подачи изменена", map[string]any{"old": oldT.ApplicationEnd, "new": newT.ApplicationEnd})
	}
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}

func (s *Store) UpdateTender(ctx context.Context, id uuid.UUID, patch map[string]any) (*Tender, error) {
	cur, err := s.GetTender(ctx, id)
	if err != nil {
		return nil, err
	}
	if v, ok := patch["object_name"].(string); ok {
		cur.ObjectName = v
	}
	if v, ok := patch["status"].(string); ok {
		cur.Status = v
	}
	if v, ok := patch["analysis_status"].(string); ok {
		cur.AnalysisStatus = v
	}
	if v, ok := patch["nmck"].(float64); ok {
		cur.NMCK = &v
	}
	tag, err := s.Pool.Exec(ctx, `
		UPDATE tenders SET object_name=$2, status=$3, analysis_status=$4::analysis_status, nmck=$5, updated_at=now()
		WHERE id=$1`, id, cur.ObjectName, cur.Status, cur.AnalysisStatus, cur.NMCK)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	_, _ = s.AddEvent(ctx, id, "updated", "тендер обновлён вручную", patch)
	return s.GetTender(ctx, id)
}

func (s *Store) DeleteTender(ctx context.Context, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM tenders WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) AddEvent(ctx context.Context, tenderID uuid.UUID, eventType, message string, details any) (uuid.UUID, error) {
	b, _ := json.Marshal(details)
	if details == nil {
		b = []byte(`{}`)
	}
	var id uuid.UUID
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO tender_events(tender_id,event_type,message,details) VALUES($1,$2,$3,$4) RETURNING id`,
		tenderID, eventType, message, b).Scan(&id)
	return id, err
}

func (s *Store) ListEvents(ctx context.Context, tenderID uuid.UUID) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id,event_type,message,details,created_at FROM tender_events
		WHERE tender_id=$1 ORDER BY created_at DESC LIMIT 200`, tenderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id uuid.UUID
		var et, msg string
		var details json.RawMessage
		var at time.Time
		if err := rows.Scan(&id, &et, &msg, &details, &at); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "event_type": et, "message": msg, "details": details, "created_at": at})
	}
	return out, rows.Err()
}

func (s *Store) UpsertDocument(ctx context.Context, d Document) (*Document, bool, error) {
	var existingID uuid.UUID
	err := s.Pool.QueryRow(ctx, `SELECT id FROM documents WHERE tender_id=$1 AND source_url=$2`, d.TenderID, d.SourceURL).Scan(&existingID)
	created := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !created {
		return nil, false, err
	}
	var out Document
	err = s.Pool.QueryRow(ctx, `
		INSERT INTO documents(tender_id,uid,filename,source_url,group_title,edition,process_status,text_content,process_error,content_hash,removed,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7::doc_process_status,$8,$9,$10,false,now())
		ON CONFLICT (tender_id, source_url) DO UPDATE SET
		  uid=COALESCE(NULLIF(EXCLUDED.uid,''), documents.uid),
		  filename=COALESCE(NULLIF(EXCLUDED.filename,''), documents.filename),
		  group_title=COALESCE(NULLIF(EXCLUDED.group_title,''), documents.group_title),
		  edition=COALESCE(NULLIF(EXCLUDED.edition,''), documents.edition),
		  process_status=EXCLUDED.process_status,
		  text_content=COALESCE(EXCLUDED.text_content, documents.text_content),
		  process_error=EXCLUDED.process_error,
		  content_hash=COALESCE(NULLIF(EXCLUDED.content_hash,''), documents.content_hash),
		  removed=false,
		  updated_at=now()
		RETURNING id,tender_id,uid,filename,source_url,group_title,edition,process_status,text_content,process_error,content_hash,removed,created_at,updated_at`,
		d.TenderID, d.UID, d.Filename, d.SourceURL, d.GroupTitle, d.Edition, d.ProcessStatus, d.TextContent, d.ProcessError, d.ContentHash,
	).Scan(&out.ID, &out.TenderID, &out.UID, &out.Filename, &out.SourceURL, &out.GroupTitle, &out.Edition, &out.ProcessStatus, &out.TextContent, &out.ProcessError, &out.ContentHash, &out.Removed, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return nil, false, err
	}
	if created {
		_, _ = s.AddEvent(ctx, d.TenderID, "document_added", out.Filename, map[string]any{"url": out.SourceURL})
	}
	return &out, created, nil
}

func (s *Store) MarkMissingDocuments(ctx context.Context, tenderID uuid.UUID, presentURLs []string) error {
	if len(presentURLs) == 0 {
		_, err := s.Pool.Exec(ctx, `UPDATE documents SET removed=true, updated_at=now() WHERE tender_id=$1 AND removed=false`, tenderID)
		return err
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE documents SET removed=true, updated_at=now()
		WHERE tender_id=$1 AND removed=false AND NOT (source_url = ANY($2))`, tenderID, presentURLs)
	return err
}

func (s *Store) ListDocuments(ctx context.Context, tenderID uuid.UUID, includeText bool) ([]Document, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id,tender_id,uid,filename,source_url,group_title,edition,process_status,
		       CASE WHEN $2 THEN text_content ELSE NULL END,
		       process_error,content_hash,removed,created_at,updated_at
		FROM documents WHERE tender_id=$1 ORDER BY filename`, tenderID, includeText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Document
	for rows.Next() {
		var d Document
		if err := rows.Scan(&d.ID, &d.TenderID, &d.UID, &d.Filename, &d.SourceURL, &d.GroupTitle, &d.Edition, &d.ProcessStatus,
			&d.TextContent, &d.ProcessError, &d.ContentHash, &d.Removed, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) GetAssessment(ctx context.Context, tenderID uuid.UUID) (*Assessment, error) {
	var a Assessment
	err := s.Pool.QueryRow(ctx, `SELECT tender_id,summary,score,details,updated_at FROM tender_assessments WHERE tender_id=$1`, tenderID).
		Scan(&a.TenderID, &a.Summary, &a.Score, &a.Details, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) UpsertAssessment(ctx context.Context, a Assessment) (*Assessment, error) {
	if a.Details == nil {
		a.Details = json.RawMessage(`{}`)
	}
	var out Assessment
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO tender_assessments(tender_id,summary,score,details,updated_at)
		VALUES($1,$2,$3,$4,now())
		ON CONFLICT (tender_id) DO UPDATE SET summary=EXCLUDED.summary, score=EXCLUDED.score, details=EXCLUDED.details, updated_at=now()
		RETURNING tender_id,summary,score,details,updated_at`, a.TenderID, a.Summary, a.Score, a.Details).
		Scan(&out.TenderID, &out.Summary, &out.Score, &out.Details, &out.UpdatedAt)
	if err != nil {
		return nil, err
	}
	_, _ = s.Pool.Exec(ctx, `UPDATE tenders SET analysis_status='analyzed', updated_at=now() WHERE id=$1`, a.TenderID)
	return &out, nil
}

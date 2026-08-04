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
		       COALESCE(t.collect_pct,0), COALESCE(t.ai_pct,0),
		       COALESCE(array_agg(DISTINCT c.slug) FILTER (WHERE c.slug IS NOT NULL), '{}'),
		       COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=t.id AND NOT d.removed),0),
		       COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=t.id AND NOT d.removed AND d.process_status='processed'),0),
		       COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=t.id AND NOT d.removed AND d.process_status='unprocessed'),0),
		       COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=t.id AND NOT d.removed AND d.text_content IS NOT NULL AND length(trim(d.text_content))>0),0),
		       COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=t.id AND NOT d.removed AND d.process_error<>''),0),
		       COALESCE((SELECT i.status::text FROM ingest_job_items i WHERE i.reg_number=t.reg_number ORDER BY i.updated_at DESC LIMIT 1),''),
		       a.score,
		       COALESCE(a.details->>'recommendation', '')
		FROM tenders t
		LEFT JOIN tender_categories tc ON tc.tender_id=t.id
		LEFT JOIN categories c ON c.id=tc.category_id
		LEFT JOIN tender_assessments a ON a.tender_id=t.id
		WHERE ($1='' OR c.slug=$1)
		  AND ($2='' OR t.reg_number ILIKE '%'||$2||'%' OR t.object_name ILIKE '%'||$2||'%')
		  AND ($3='' OR t.analysis_status::text=$3)
		GROUP BY t.id, a.score, a.details
		ORDER BY t.application_end NULLS LAST, t.updated_at DESC
		LIMIT $4`, categorySlug, strings.TrimSpace(q), strings.TrimSpace(status), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tender
	for rows.Next() {
		var t Tender
		var rec string
		if err := rows.Scan(&t.ID, &t.RegNumber, &t.SourceSite, &t.Law, &t.CustomerID, &t.ObjectName, &t.Status, &t.NMCK, &t.Currency,
			&t.PublishedAt, &t.UpdatedOnSite, &t.ApplicationEnd, &t.AnalysisStatus, &t.Payload, &t.CreatedAt, &t.UpdatedAt,
			&t.StoredCollectPct, &t.StoredAIPct,
			&t.CategorySlugs,
			&t.DocsTotal, &t.DocsProcessed, &t.DocsUnprocessed, &t.DocsWithText, &t.DocsErrors,
			&t.IngestStatus, &t.AssessScore, &rec); err != nil {
			return nil, err
		}
		t.Recommendation = rec
		enrichTenderProgress(&t)
		out = append(out, t)
	}
	return out, rows.Err()
}

func enrichTenderProgress(t *Tender) {
	// Сбор: плавный % из БД во время работы, иначе вычисляем.
	switch {
	case t.IngestStatus == "running" || t.IngestStatus == "queued":
		if t.StoredCollectPct > 0 {
			t.CollectPct = t.StoredCollectPct
		} else if t.DocsTotal > 0 {
			t.CollectPct = 15 + int(float64(t.DocsProcessed)/float64(maxInt(t.DocsTotal, 1))*70)
		} else {
			t.CollectPct = 8
		}
	case t.DocsTotal > 0 && t.DocsUnprocessed == 0 && t.DocsErrors == 0:
		t.CollectPct = 100
		ok := true
		t.CollectOK = &ok
	case t.DocsTotal > 0 && t.DocsUnprocessed == 0 && t.DocsErrors > 0:
		t.CollectPct = 100
		ok := false
		t.CollectOK = &ok
	case t.IngestStatus == "ok":
		// Парсер закончил карточку — сбор файлов завершён (обработка текста может быть частичной).
		t.CollectPct = 100
		if t.DocsErrors > 0 {
			ok := false
			t.CollectOK = &ok
		} else {
			ok := true
			t.CollectOK = &ok
		}
	case t.IngestStatus == "error" || t.IngestStatus == "failed_analyze" || t.IngestStatus == "unsupported_source":
		t.CollectPct = maxInt(t.StoredCollectPct, 30)
		ok := false
		t.CollectOK = &ok
	case t.DocsTotal > 0:
		t.CollectPct = 20 + int(float64(t.DocsProcessed)/float64(t.DocsTotal)*75)
	default:
		t.CollectPct = t.StoredCollectPct
	}

	// AI: во время анализа берём % из БД (обновляется по порциям LM Studio).
	switch t.AnalysisStatus {
	case "analyzed":
		t.AIPct = 100
		ok := true
		t.AIOK = &ok
	case "other":
		t.AIPct = 100
		ok := false
		t.AIOK = &ok
	case "analyzing":
		if t.StoredAIPct > 0 {
			t.AIPct = t.StoredAIPct
		} else {
			t.AIPct = 5
		}
	default:
		t.AIPct = 0
	}

	// Готов к AI: ingest ok (парсер ушёл дальше), есть хотя бы один текст — не ждём обработки всех файлов.
	t.ReadyForAI = t.IngestStatus == "ok" && t.DocsWithText > 0 &&
		(t.AnalysisStatus == "none" || t.AnalysisStatus == "")

	incompleteDownload := t.IngestStatus == "running" || t.IngestStatus == "queued" ||
		(t.IngestStatus != "ok" && t.IngestStatus != "" && t.IngestStatus != "error" && t.IngestStatus != "failed_analyze" && t.IngestStatus != "unsupported_source" && t.IngestStatus != "cancelled" && t.DocsTotal == 0) ||
		(t.IngestStatus == "ok" && t.DocsTotal == 0)

	switch {
	case incompleteDownload || (t.IngestStatus == "running" || t.IngestStatus == "queued"):
		t.CardTone = "pending"
	case t.CollectOK != nil && !*t.CollectOK:
		t.CardTone = "bad"
	case t.AIOK != nil && !*t.AIOK:
		t.CardTone = "bad"
	case t.Recommendation == "skip" || t.AnalysisStatus == "irrelevant" || t.AnalysisStatus == "delete":
		t.CardTone = "bad"
	case t.CollectOK != nil && *t.CollectOK && t.DocsWithText > 0:
		t.CardTone = "good"
	case t.AnalysisStatus == "analyzed" && (t.Recommendation == "participate" || t.Recommendation == "caution"):
		t.CardTone = "good"
	default:
		t.CardTone = "neutral"
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// NextTenderReadyForAI — ingest завершён (ok), есть текст; не ждём process_status всех документов.
func (s *Store) NextTenderReadyForAI(ctx context.Context) (*Tender, error) {
	var id uuid.UUID
	err := s.Pool.QueryRow(ctx, `
		SELECT t.id FROM tenders t
		WHERE t.analysis_status = 'none'
		  AND EXISTS (
		    SELECT 1 FROM ingest_job_items i
		    WHERE i.reg_number=t.reg_number AND i.status='ok'
		      AND i.updated_at = (
		        SELECT max(i2.updated_at) FROM ingest_job_items i2 WHERE i2.reg_number=t.reg_number
		      )
		  )
		  AND EXISTS (
		    SELECT 1 FROM documents d
		    WHERE d.tender_id=t.id AND NOT d.removed
		      AND d.text_content IS NOT NULL AND length(trim(d.text_content))>0
		  )
		ORDER BY t.updated_at ASC
		LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.GetTender(ctx, id)
}

// MarkAnalyzing — атомарно забирает карточку под авто-AI (только из none).
func (s *Store) MarkAnalyzing(ctx context.Context, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE tenders SET analysis_status='analyzing', updated_at=now() WHERE id=$1 AND analysis_status='none'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("tender not claimed for analyzing")
	}
	return nil
}

// RequeueStuckAnalyzing — после рестарта core вернуть «зависшие» analyzing → none.
func (s *Store) RequeueStuckAnalyzing(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE tenders SET analysis_status='none', updated_at=now() WHERE analysis_status='analyzing'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// EnrichTenderUI заполняет прогресс/тон для одной карточки (модалка).
func (s *Store) EnrichTenderUI(ctx context.Context, t *Tender) error {
	err := s.Pool.QueryRow(ctx, `
		SELECT
		  COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=$1 AND NOT d.removed),0),
		  COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=$1 AND NOT d.removed AND d.process_status='processed'),0),
		  COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=$1 AND NOT d.removed AND d.process_status='unprocessed'),0),
		  COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=$1 AND NOT d.removed AND d.text_content IS NOT NULL AND length(trim(d.text_content))>0),0),
		  COALESCE((SELECT count(*) FROM documents d WHERE d.tender_id=$1 AND NOT d.removed AND d.process_error<>''),0),
		  COALESCE((SELECT i.status::text FROM ingest_job_items i WHERE i.reg_number=$2 ORDER BY i.updated_at DESC LIMIT 1),''),
		  a.score,
		  COALESCE(a.details->>'recommendation', ''),
		  COALESCE(t.collect_pct,0),
		  COALESCE(t.ai_pct,0)
		FROM tenders t
		LEFT JOIN tender_assessments a ON a.tender_id=t.id
		WHERE t.id=$1`, t.ID, t.RegNumber).
		Scan(&t.DocsTotal, &t.DocsProcessed, &t.DocsUnprocessed, &t.DocsWithText, &t.DocsErrors,
			&t.IngestStatus, &t.AssessScore, &t.Recommendation, &t.StoredCollectPct, &t.StoredAIPct)
	if err != nil {
		return err
	}
	enrichTenderProgress(t)
	return nil
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

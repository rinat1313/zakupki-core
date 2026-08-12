package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	RetainReasonManual        = "manual"
	RetainReasonInteresting   = "interesting"
	RetainReasonInWork        = "in_work"
	RetainReasonAIInteresting = "ai_interesting"
	RetainReasonAnalyzing     = "analyzing"
)

// NormalizeRetainReason maps free-form reason to a stable value.
func NormalizeRetainReason(reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "", RetainReasonManual:
		return RetainReasonManual
	case "interesting", "liked", "favorite", "star":
		return RetainReasonInteresting
	case "in_work", "work", "working":
		return RetainReasonInWork
	case "ai_interesting", "ai", "participate", "caution":
		return RetainReasonAIInteresting
	case "analyzing", "analyze", "analysis":
		return RetainReasonAnalyzing
	default:
		return strings.TrimSpace(reason)
	}
}

// RetainTender marks a tender as workspace-retained so search-pool sync won't delete it.
func (s *Store) RetainTender(ctx context.Context, id uuid.UUID, reason string) (*Tender, error) {
	reason = NormalizeRetainReason(reason)
	tag, err := s.Pool.Exec(ctx, `
		UPDATE tenders SET
		  retained=true,
		  retained_at=COALESCE(retained_at, now()),
		  retain_reason=CASE
		    WHEN NOT retained THEN $2
		    WHEN retain_reason IN ('', 'manual', 'analyzing') THEN $2
		    WHEN $2 IN ('in_work','interesting','ai_interesting') THEN $2
		    ELSE retain_reason
		  END,
		  updated_at=now()
		WHERE id=$1`, id, reason)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	_, _ = s.AddEvent(ctx, id, "retained", "тендер сохранён вне пула поиска", map[string]any{"reason": reason})
	return s.GetTender(ctx, id)
}

// UnretainTender removes workspace retention.
func (s *Store) UnretainTender(ctx context.Context, id uuid.UUID) (*Tender, error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE tenders SET retained=false, retained_at=NULL, retain_reason='', updated_at=now()
		WHERE id=$1`, id)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	_, _ = s.AddEvent(ctx, id, "unretained", "снято сохранение вне пула поиска", nil)
	return s.GetTender(ctx, id)
}

// SyncItem — хит из zakupki-search.
type SyncItem struct {
	Reg        string
	Site       string
	Law        string
	ObjectName string
}

type SyncSearchPoolResult struct {
	CategoryID          uuid.UUID  `json:"category_id"`
	Upserted            int        `json:"upserted"`
	Enqueued            int        `json:"enqueued"`
	JobID               *uuid.UUID `json:"job_id,omitempty"`
	RemovedFromPool     int        `json:"removed_from_pool"`
	KeptRetained        int        `json:"kept_retained"`
	Deleted             int        `json:"deleted"`
	SkippedSameVersion  bool       `json:"skipped_same_version,omitempty"`
	SyncedConfigVersion int64      `json:"synced_config_version"`
	Job                 *IngestJob `json:"-"`
}

type SyncSearchPoolOpts struct {
	Items         []SyncItem
	Enqueue       bool
	SourceName    string
	ConfigVersion int64 // 0 = always apply
	Title         string
}

// SyncSearchConfig — полный sync пула поисковика:
// ensure category → upsert tenders + tender_categories → prune missing → optional ingest job.
func (s *Store) SyncSearchConfig(ctx context.Context, searchConfigID string, opts SyncSearchPoolOpts) (*SyncSearchPoolResult, error) {
	cat, err := s.EnsureCategoryBySearchConfig(ctx, searchConfigID, opts.Title)
	if err != nil {
		return nil, err
	}
	return s.SyncSearchPoolOpts(ctx, cat.ID, opts)
}

// SyncSearchPool replaces membership for an existing categoryID.
func (s *Store) SyncSearchPool(ctx context.Context, categoryID uuid.UUID, items []struct{ Reg, Site string }, enqueue bool, sourceName string) (*SyncSearchPoolResult, error) {
	rich := make([]SyncItem, 0, len(items))
	for _, it := range items {
		rich = append(rich, SyncItem{Reg: it.Reg, Site: it.Site})
	}
	return s.SyncSearchPoolOpts(ctx, categoryID, SyncSearchPoolOpts{
		Items: rich, Enqueue: enqueue, SourceName: sourceName,
	})
}

func (s *Store) SyncSearchPoolOpts(ctx context.Context, categoryID uuid.UUID, opts SyncSearchPoolOpts) (*SyncSearchPoolResult, error) {
	out := &SyncSearchPoolResult{CategoryID: categoryID}

	if opts.ConfigVersion > 0 {
		var cur int64
		err := s.Pool.QueryRow(ctx, `SELECT COALESCE(synced_config_version,0) FROM categories WHERE id=$1`, categoryID).Scan(&cur)
		if err != nil {
			return nil, err
		}
		if cur >= opts.ConfigVersion {
			out.SkippedSameVersion = true
			out.SyncedConfigVersion = cur
			return out, nil
		}
	}

	wanted := map[string]struct{}{}
	normItems := make([]SyncItem, 0, len(opts.Items))
	for _, it := range opts.Items {
		reg := strings.TrimSpace(it.Reg)
		site := strings.TrimSpace(it.Site)
		if reg == "" {
			continue
		}
		if site == "" {
			site = "https://zakupki.gov.ru"
		}
		key := reg + "\x00" + site
		if _, ok := wanted[key]; ok {
			continue
		}
		wanted[key] = struct{}{}
		normItems = append(normItems, SyncItem{
			Reg: reg, Site: site,
			Law: strings.TrimSpace(it.Law), ObjectName: strings.TrimSpace(it.ObjectName),
		})
	}

	// 1) Upsert each hit so GET /tenders?search_config_id= immediately returns rows.
	for _, it := range normItems {
		_, _, err := s.UpsertTender(ctx, TenderUpsertInput{
			RegNumber:  it.Reg,
			SourceSite: it.Site,
			Law:        it.Law,
			ObjectName: it.ObjectName,
			CategoryID: categoryID,
			Payload:    []byte(`{}`),
		})
		if err != nil {
			return out, fmt.Errorf("upsert %s: %w", it.Reg, err)
		}
		out.Upserted++
	}

	// 2) Prune tenders that fell out of the search snapshot (keep retained).
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT t.id, t.reg_number, t.source_site, COALESCE(t.retained,false)
		FROM tender_categories tc
		JOIN tenders t ON t.id=tc.tender_id
		WHERE tc.category_id=$1`, categoryID)
	if err != nil {
		return nil, err
	}
	type curRow struct {
		id       uuid.UUID
		reg      string
		site     string
		retained bool
	}
	var current []curRow
	for rows.Next() {
		var r curRow
		if err := rows.Scan(&r.id, &r.reg, &r.site, &r.retained); err != nil {
			rows.Close()
			return nil, err
		}
		current = append(current, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, r := range current {
		key := r.reg + "\x00" + r.site
		if _, ok := wanted[key]; ok {
			continue
		}
		if _, err := tx.Exec(ctx, `DELETE FROM tender_categories WHERE tender_id=$1 AND category_id=$2`, r.id, categoryID); err != nil {
			return nil, err
		}
		out.RemovedFromPool++
		if r.retained {
			out.KeptRetained++
			continue
		}
		var left int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM tender_categories WHERE tender_id=$1`, r.id).Scan(&left); err != nil {
			return nil, err
		}
		if left == 0 {
			tag, err := tx.Exec(ctx, `DELETE FROM tenders WHERE id=$1 AND retained=false`, r.id)
			if err != nil {
				return nil, err
			}
			out.Deleted += int(tag.RowsAffected())
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	if opts.ConfigVersion > 0 {
		if err := s.SetSyncedConfigVersion(ctx, categoryID, opts.ConfigVersion); err != nil {
			return out, err
		}
		out.SyncedConfigVersion = opts.ConfigVersion
	} else {
		_ = s.Pool.QueryRow(ctx, `SELECT COALESCE(synced_config_version,0) FROM categories WHERE id=$1`, categoryID).
			Scan(&out.SyncedConfigVersion)
	}

	// 3) Optional ingest job so workers enrich cards via parser.
	if opts.Enqueue && len(normItems) > 0 {
		sourceName := opts.SourceName
		if sourceName == "" {
			sourceName = "search-sync"
		}
		plain := make([]struct{ Reg, Site string }, 0, len(normItems))
		for _, it := range normItems {
			plain = append(plain, struct{ Reg, Site string }{Reg: it.Reg, Site: it.Site})
		}
		job, err := s.CreateIngestJob(ctx, categoryID, sourceName, plain)
		if err != nil {
			return out, fmt.Errorf("pool synced but enqueue failed: %w", err)
		}
		out.Job = job
		out.JobID = &job.ID
		out.Enqueued = len(normItems)
	}
	return out, nil
}

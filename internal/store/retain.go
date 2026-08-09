package store

import (
	"context"
	"fmt"
	"strings"
	"time"

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
// If already retained, reason is upgraded only when the previous reason was weaker.
func (s *Store) RetainTender(ctx context.Context, id uuid.UUID, reason string) (*Tender, error) {
	reason = NormalizeRetainReason(reason)
	now := time.Now().UTC()
	tag, err := s.Pool.Exec(ctx, `
		UPDATE tenders SET
		  retained=true,
		  retained_at=COALESCE(retained_at, $2),
		  retain_reason=CASE
		    WHEN NOT retained THEN $3
		    WHEN retain_reason IN ('', 'manual', 'analyzing') THEN $3
		    WHEN $3 IN ('in_work','interesting','ai_interesting') THEN $3
		    ELSE retain_reason
		  END,
		  updated_at=now()
		WHERE id=$1`, id, now, reason)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	_, _ = s.AddEvent(ctx, id, "retained", "тендер сохранён вне пула поиска", map[string]any{"reason": reason})
	return s.GetTender(ctx, id)
}

// UnretainTender removes workspace retention. Tender may then be deleted on next search sync
// if it is no longer in the search pool.
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

type SyncSearchPoolResult struct {
	RemovedFromPool     int        `json:"removed_from_pool"`
	KeptRetained        int        `json:"kept_retained"`
	Deleted             int        `json:"deleted"`
	Enqueued            int        `json:"enqueued"`
	SkippedSameVersion  bool       `json:"skipped_same_version,omitempty"`
	SyncedConfigVersion int64      `json:"synced_config_version"`
	Job                 *IngestJob `json:"job,omitempty"`
}

type SyncSearchPoolOpts struct {
	Items         []struct{ Reg, Site string }
	Enqueue       bool
	SourceName    string
	ConfigVersion int64 // 0 = always apply
}

// SyncSearchPool replaces the searcher-managed membership of a category.
// Tenders absent from `items`:
//   - if retained → unlinked from category, kept in DB (workspace)
//   - if not retained and no other categories → deleted
// When enqueue=true, creates an ingest job for the new pool items.
func (s *Store) SyncSearchPool(ctx context.Context, categoryID uuid.UUID, items []struct{ Reg, Site string }, enqueue bool, sourceName string) (*SyncSearchPoolResult, error) {
	return s.SyncSearchPoolOpts(ctx, categoryID, SyncSearchPoolOpts{
		Items: items, Enqueue: enqueue, SourceName: sourceName,
	})
}

func (s *Store) SyncSearchPoolOpts(ctx context.Context, categoryID uuid.UUID, opts SyncSearchPoolOpts) (*SyncSearchPoolResult, error) {
	items := opts.Items
	enqueue := opts.Enqueue
	sourceName := opts.SourceName

	if opts.ConfigVersion > 0 {
		var cur int64
		err := s.Pool.QueryRow(ctx, `SELECT COALESCE(synced_config_version,0) FROM categories WHERE id=$1`, categoryID).Scan(&cur)
		if err != nil {
			return nil, err
		}
		if cur >= opts.ConfigVersion {
			return &SyncSearchPoolResult{
				SkippedSameVersion:  true,
				SyncedConfigVersion: cur,
			}, nil
		}
	}
	wanted := map[string]struct{}{}
	normItems := make([]struct{ Reg, Site string }, 0, len(items))
	for _, it := range items {
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
		normItems = append(normItems, struct{ Reg, Site string }{Reg: reg, Site: site})
	}

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

	out := &SyncSearchPoolResult{}
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

	if enqueue && len(normItems) > 0 {
		if sourceName == "" {
			sourceName = "search-sync"
		}
		job, err := s.CreateIngestJob(ctx, categoryID, sourceName, normItems)
		if err != nil {
			return out, fmt.Errorf("pool synced but enqueue failed: %w", err)
		}
		out.Job = job
		out.Enqueued = len(normItems)
	}
	return out, nil
}

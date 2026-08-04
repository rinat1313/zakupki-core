package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateIngestJob(ctx context.Context, categoryID uuid.UUID, sourceName string, items []struct{ Reg, Site string }) (*IngestJob, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var job IngestJob
	err = tx.QueryRow(ctx, `
		INSERT INTO ingest_jobs(category_id, source_name, status, total_items)
		VALUES($1,$2,'queued',$3)
		RETURNING id,category_id,source_name,status,total_items,done_items,error_items,created_at,updated_at`,
		categoryID, sourceName, len(items)).
		Scan(&job.ID, &job.CategoryID, &job.SourceName, &job.Status, &job.TotalItems, &job.DoneItems, &job.ErrorItems, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		reg := strings.TrimSpace(it.Reg)
		site := strings.TrimSpace(it.Site)
		if reg == "" {
			continue
		}
		if site == "" {
			site = "https://zakupki.gov.ru"
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO ingest_job_items(job_id,reg_number,source_site,status)
			VALUES($1,$2,$3,'queued')`, job.ID, reg, site)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Store) ListJobs(ctx context.Context, limit int) ([]IngestJob, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT j.id,j.category_id,c.slug,c.title,j.source_name,j.status,j.total_items,j.done_items,j.error_items,j.created_at,j.updated_at
		FROM ingest_jobs j JOIN categories c ON c.id=j.category_id
		ORDER BY j.created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IngestJob
	for rows.Next() {
		var j IngestJob
		if err := rows.Scan(&j.ID, &j.CategoryID, &j.CategorySlug, &j.CategoryTitle, &j.SourceName, &j.Status, &j.TotalItems, &j.DoneItems, &j.ErrorItems, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (*IngestJob, []IngestItem, error) {
	var j IngestJob
	err := s.Pool.QueryRow(ctx, `
		SELECT j.id,j.category_id,c.slug,c.title,j.source_name,j.status,j.total_items,j.done_items,j.error_items,j.created_at,j.updated_at
		FROM ingest_jobs j JOIN categories c ON c.id=j.category_id WHERE j.id=$1`, id).
		Scan(&j.ID, &j.CategoryID, &j.CategorySlug, &j.CategoryTitle, &j.SourceName, &j.Status, &j.TotalItems, &j.DoneItems, &j.ErrorItems, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT id,job_id,reg_number,source_site,status,error,tender_id,created_at,updated_at
		FROM ingest_job_items WHERE job_id=$1 ORDER BY created_at`, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var items []IngestItem
	for rows.Next() {
		var it IngestItem
		if err := rows.Scan(&it.ID, &it.JobID, &it.RegNumber, &it.SourceSite, &it.Status, &it.Error, &it.TenderID, &it.CreatedAt, &it.UpdatedAt); err != nil {
			return nil, nil, err
		}
		items = append(items, it)
	}
	return &j, items, rows.Err()
}

func (s *Store) ClaimNextItem(ctx context.Context) (*IngestItem, *IngestJob, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	var it IngestItem
	err = tx.QueryRow(ctx, `
		SELECT id,job_id,reg_number,source_site,status,error,tender_id,created_at,updated_at
		FROM ingest_job_items WHERE status='queued'
		ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&it.ID, &it.JobID, &it.RegNumber, &it.SourceSite, &it.Status, &it.Error, &it.TenderID, &it.CreatedAt, &it.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	_, err = tx.Exec(ctx, `UPDATE ingest_job_items SET status='running', updated_at=now() WHERE id=$1`, it.ID)
	if err != nil {
		return nil, nil, err
	}
	_, _ = tx.Exec(ctx, `UPDATE ingest_jobs SET status='running', updated_at=now() WHERE id=$1`, it.JobID)

	var job IngestJob
	err = tx.QueryRow(ctx, `
		SELECT j.id,j.category_id,c.slug,c.title,j.source_name,j.status,j.total_items,j.done_items,j.error_items,j.created_at,j.updated_at
		FROM ingest_jobs j JOIN categories c ON c.id=j.category_id WHERE j.id=$1`, it.JobID).
		Scan(&job.ID, &job.CategoryID, &job.CategorySlug, &job.CategoryTitle, &job.SourceName, &job.Status, &job.TotalItems, &job.DoneItems, &job.ErrorItems, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	it.Status = "running"
	return &it, &job, nil
}

func (s *Store) FinishItem(ctx context.Context, itemID uuid.UUID, status string, errMsg string, tenderID *uuid.UUID) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var jobID uuid.UUID
	err = tx.QueryRow(ctx, `
		UPDATE ingest_job_items SET status=$2::ingest_item_status, error=$3, tender_id=$4, updated_at=now()
		WHERE id=$1 AND status='running' RETURNING job_id`, itemID, status, errMsg, tenderID).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		// item already cancelled/finished (e.g. stop while in flight)
		return nil
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE ingest_jobs SET
		  done_items = (SELECT count(*) FROM ingest_job_items WHERE job_id=$1 AND status IN ('ok','skipped','unsupported_source','failed_analyze','cancelled')),
		  error_items = (SELECT count(*) FROM ingest_job_items WHERE job_id=$1 AND status='error'),
		  status = CASE
		    WHEN (SELECT count(*) FROM ingest_job_items WHERE job_id=$1 AND status IN ('queued','running'))=0 THEN 'done'
		    ELSE 'running'
		  END,
		  updated_at=now()
		WHERE id=$1 AND status <> 'cancelled'`, jobID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RequeueItem returns a running item to the queue (used when ingest is paused mid-claim).
func (s *Store) RequeueItem(ctx context.Context, itemID uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE ingest_job_items SET status='queued', updated_at=now()
		WHERE id=$1 AND status='running'`, itemID)
	return err
}

// CancelActiveIngest marks queued/running items and their jobs as cancelled.
func (s *Store) CancelActiveIngest(ctx context.Context) (items int64, jobs int64, err error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE ingest_job_items
		SET status='cancelled', error='stopped by user', updated_at=now()
		WHERE status IN ('queued','running')`)
	if err != nil {
		return 0, 0, err
	}
	items = tag.RowsAffected()

	tag, err = tx.Exec(ctx, `
		UPDATE ingest_jobs SET
		  done_items = (SELECT count(*) FROM ingest_job_items WHERE job_id=ingest_jobs.id AND status IN ('ok','skipped','unsupported_source','failed_analyze','cancelled')),
		  error_items = (SELECT count(*) FROM ingest_job_items WHERE job_id=ingest_jobs.id AND status='error'),
		  status = 'cancelled',
		  updated_at = now()
		WHERE status IN ('queued','running')`)
	if err != nil {
		return 0, 0, err
	}
	jobs = tag.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return items, jobs, nil
}

// RequeueStuckRunning returns items left in running after a crash/restart back to queued.
func (s *Store) RequeueStuckRunning(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE ingest_job_items SET status='queued', updated_at=now()
		WHERE status='running'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

type IngestStatsRow struct {
	CategorySlug  string `json:"category_slug"`
	CategoryTitle string `json:"category_title"`
	Queued        int    `json:"queued"`
	Running       int    `json:"running"`
	OK            int    `json:"ok"`
	Error         int    `json:"error"`
	Skipped       int    `json:"skipped"`
	Unsupported   int    `json:"unsupported_source"`
	FailedAnalyze int    `json:"failed_analyze"`
	TendersInDB   int    `json:"tenders_in_db"`
}

func (s *Store) IngestStats(ctx context.Context) ([]IngestStatsRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT c.slug, c.title,
		  COALESCE(SUM(CASE WHEN i.status='queued' THEN 1 ELSE 0 END),0),
		  COALESCE(SUM(CASE WHEN i.status='running' THEN 1 ELSE 0 END),0),
		  COALESCE(SUM(CASE WHEN i.status='ok' THEN 1 ELSE 0 END),0),
		  COALESCE(SUM(CASE WHEN i.status='error' THEN 1 ELSE 0 END),0),
		  COALESCE(SUM(CASE WHEN i.status='skipped' THEN 1 ELSE 0 END),0),
		  COALESCE(SUM(CASE WHEN i.status='unsupported_source' THEN 1 ELSE 0 END),0),
		  COALESCE(SUM(CASE WHEN i.status='failed_analyze' THEN 1 ELSE 0 END),0),
		  (SELECT count(DISTINCT tc.tender_id) FROM tender_categories tc WHERE tc.category_id=c.id)
		FROM categories c
		LEFT JOIN ingest_jobs j ON j.category_id=c.id
		LEFT JOIN ingest_job_items i ON i.job_id=j.id
		GROUP BY c.id, c.slug, c.title
		ORDER BY c.title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IngestStatsRow
	for rows.Next() {
		var r IngestStatsRow
		if err := rows.Scan(&r.CategorySlug, &r.CategoryTitle, &r.Queued, &r.Running, &r.OK, &r.Error, &r.Skipped, &r.Unsupported, &r.FailedAnalyze, &r.TendersInDB); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type JobLog struct {
	ID        int64     `json:"id"`
	JobID     uuid.UUID `json:"job_id"`
	ItemID    *uuid.UUID `json:"item_id,omitempty"`
	RegNumber string    `json:"reg_number"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) AddJobLog(ctx context.Context, jobID, itemID uuid.UUID, reg, level, message string) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO ingest_job_logs(job_id, item_id, reg_number, level, message)
		VALUES($1,$2,$3,$4,$5)`, jobID, itemID, reg, level, message)
	return err
}

func (s *Store) ListJobLogs(ctx context.Context, jobID uuid.UUID, afterID int64, limit int) ([]JobLog, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT id, job_id, item_id, reg_number, level, message, created_at
		FROM ingest_job_logs WHERE job_id=$1 AND id>$2
		ORDER BY id ASC LIMIT $3`, jobID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobLog
	for rows.Next() {
		var l JobLog
		if err := rows.Scan(&l.ID, &l.JobID, &l.ItemID, &l.RegNumber, &l.Level, &l.Message, &l.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *Store) DeleteJob(ctx context.Context, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM ingest_jobs WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteJobsByCategory(ctx context.Context, categoryID uuid.UUID) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM ingest_jobs WHERE category_id=$1`, categoryID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) ClearCategoryTenders(ctx context.Context, categoryID uuid.UUID) (int64, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT tender_id FROM tender_categories WHERE category_id=$1`, categoryID)
	if err != nil {
		return 0, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	_, err = tx.Exec(ctx, `DELETE FROM tender_categories WHERE category_id=$1`, categoryID)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, id := range ids {
		var left int
		_ = tx.QueryRow(ctx, `SELECT count(*) FROM tender_categories WHERE tender_id=$1`, id).Scan(&left)
		if left == 0 {
			tag, err := tx.Exec(ctx, `DELETE FROM tenders WHERE id=$1`, id)
			if err != nil {
				return 0, err
			}
			n += tag.RowsAffected()
		}
	}
	return n, tx.Commit(ctx)
}

// EnqueueRefresh создаёт job на повторный сбор выбранных тендеров категории.
func (s *Store) EnqueueRefresh(ctx context.Context, categoryID uuid.UUID, statuses []string, tenderIDs []uuid.UUID) (*IngestJob, error) {
	q := `
		SELECT t.reg_number, t.source_site FROM tenders t
		JOIN tender_categories tc ON tc.tender_id=t.id
		WHERE tc.category_id=$1`
	args := []any{categoryID}
	argn := 2
	if len(tenderIDs) > 0 {
		q += fmt.Sprintf(` AND t.id = ANY($%d)`, argn)
		args = append(args, tenderIDs)
		argn++
	}
	if len(statuses) > 0 {
		q += fmt.Sprintf(` AND t.analysis_status::text = ANY($%d)`, argn)
		args = append(args, statuses)
	}
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []struct{ Reg, Site string }
	for rows.Next() {
		var reg, site string
		if err := rows.Scan(&reg, &site); err != nil {
			return nil, err
		}
		items = append(items, struct{ Reg, Site string }{reg, site})
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("no tenders matched refresh filters")
	}
	return s.CreateIngestJob(ctx, categoryID, "refresh", items)
}

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

// AIConfig — именованная конфигурация промптов для категории (ключа).
type AIConfig struct {
	ID           uuid.UUID `json:"id"`
	CategoryID   uuid.UUID `json:"category_id"`
	CategorySlug string    `json:"category_slug,omitempty"`
	Name         string    `json:"name"`
	SystemPrompt string    `json:"system_prompt"`
	UserPrompt   string    `json:"user_prompt"`
	Rules        string    `json:"rules"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (s *Store) ListAIConfigs(ctx context.Context, categoryID uuid.UUID) ([]AIConfig, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT c.id, c.category_id, cat.slug, c.name, c.system_prompt, c.user_prompt, c.rules, c.created_at, c.updated_at
		FROM ai_configs c
		JOIN categories cat ON cat.id=c.category_id
		WHERE c.category_id=$1
		ORDER BY c.name`, categoryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AIConfig
	for rows.Next() {
		var a AIConfig
		if err := rows.Scan(&a.ID, &a.CategoryID, &a.CategorySlug, &a.Name, &a.SystemPrompt, &a.UserPrompt, &a.Rules, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) GetAIConfig(ctx context.Context, id uuid.UUID) (*AIConfig, error) {
	var a AIConfig
	err := s.Pool.QueryRow(ctx, `
		SELECT c.id, c.category_id, cat.slug, c.name, c.system_prompt, c.user_prompt, c.rules, c.created_at, c.updated_at
		FROM ai_configs c
		JOIN categories cat ON cat.id=c.category_id
		WHERE c.id=$1`, id).
		Scan(&a.ID, &a.CategoryID, &a.CategorySlug, &a.Name, &a.SystemPrompt, &a.UserPrompt, &a.Rules, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) CreateAIConfig(ctx context.Context, categoryID uuid.UUID, name, system, user, rules string) (*AIConfig, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("нужно имя конфигурации")
	}
	var a AIConfig
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO ai_configs(category_id, name, system_prompt, user_prompt, rules)
		VALUES($1,$2,$3,$4,$5)
		RETURNING id, category_id, name, system_prompt, user_prompt, rules, created_at, updated_at`,
		categoryID, name, system, user, rules).
		Scan(&a.ID, &a.CategoryID, &a.Name, &a.SystemPrompt, &a.UserPrompt, &a.Rules, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) UpdateAIConfig(ctx context.Context, id uuid.UUID, name, system, user, rules string) (*AIConfig, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("нужно имя конфигурации")
	}
	var a AIConfig
	err := s.Pool.QueryRow(ctx, `
		UPDATE ai_configs SET name=$2, system_prompt=$3, user_prompt=$4, rules=$5, updated_at=now()
		WHERE id=$1
		RETURNING id, category_id, name, system_prompt, user_prompt, rules, created_at, updated_at`,
		id, name, system, user, rules).
		Scan(&a.ID, &a.CategoryID, &a.Name, &a.SystemPrompt, &a.UserPrompt, &a.Rules, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) DeleteAIConfig(ctx context.Context, id uuid.UUID) error {
	_, _ = s.Pool.Exec(ctx, `UPDATE categories SET active_ai_config_id=NULL WHERE active_ai_config_id=$1`, id)
	tag, err := s.Pool.Exec(ctx, `DELETE FROM ai_configs WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) SetCategoryActiveAIConfig(ctx context.Context, categoryID uuid.UUID, configID *uuid.UUID) error {
	if configID != nil {
		var cat uuid.UUID
		err := s.Pool.QueryRow(ctx, `SELECT category_id FROM ai_configs WHERE id=$1`, *configID).Scan(&cat)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if cat != categoryID {
			return fmt.Errorf("конфигурация принадлежит другой категории")
		}
	}
	_, err := s.Pool.Exec(ctx, `UPDATE categories SET active_ai_config_id=$2 WHERE id=$1`, categoryID, configID)
	return err
}

// ActiveAIConfigForTender — активная AI-конфигурация категории тендера (если задана).
func (s *Store) ActiveAIConfigForTender(ctx context.Context, tenderID uuid.UUID) (*AIConfig, error) {
	var a AIConfig
	err := s.Pool.QueryRow(ctx, `
		SELECT c.id, c.category_id, cat.slug, c.name, c.system_prompt, c.user_prompt, c.rules, c.created_at, c.updated_at
		FROM tenders t
		JOIN tender_categories tc ON tc.tender_id=t.id
		JOIN categories cat ON cat.id=tc.category_id
		JOIN ai_configs c ON c.id=cat.active_ai_config_id
		WHERE t.id=$1
		ORDER BY cat.created_at
		LIMIT 1`, tenderID).
		Scan(&a.ID, &a.CategoryID, &a.CategorySlug, &a.Name, &a.SystemPrompt, &a.UserPrompt, &a.Rules, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) CountQueuedIngest(ctx context.Context) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM ingest_job_items WHERE status IN ('queued','running')`).Scan(&n)
	return n, err
}

func (s *Store) SetTenderProgress(ctx context.Context, id uuid.UUID, collectPct, aiPct *int) error {
	if collectPct == nil && aiPct == nil {
		return nil
	}
	if collectPct != nil && aiPct != nil {
		_, err := s.Pool.Exec(ctx, `UPDATE tenders SET collect_pct=$2, ai_pct=$3, updated_at=now() WHERE id=$1`, id, clampPct(*collectPct), clampPct(*aiPct))
		return err
	}
	if collectPct != nil {
		_, err := s.Pool.Exec(ctx, `UPDATE tenders SET collect_pct=$2, updated_at=now() WHERE id=$1`, id, clampPct(*collectPct))
		return err
	}
	_, err := s.Pool.Exec(ctx, `UPDATE tenders SET ai_pct=$2, updated_at=now() WHERE id=$1`, id, clampPct(*aiPct))
	return err
}

func clampPct(n int) int {
	if n < 0 {
		return 0
	}
	if n > 100 {
		return 100
	}
	return n
}

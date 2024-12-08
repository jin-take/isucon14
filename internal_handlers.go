package main

import (
	"database/sql"
	"errors"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 最も古いリクエストを取得 (最初に chair_id が NULL のレコード)
	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `
		SELECT id
		FROM rides
		WHERE chair_id IS NULL
		ORDER BY created_at ASC
		LIMIT 1
	`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 条件を満たす椅子を効率的に取得
	matched := &Chair{}
	if err := db.GetContext(ctx, matched, `
		SELECT chairs.id
		FROM chairs
		WHERE is_active = TRUE
		AND NOT EXISTS (
			SELECT 1
			FROM ride_statuses
			WHERE ride_id IN (
				SELECT id FROM rides WHERE chair_id = chairs.id
			)
			GROUP BY ride_id
			HAVING COUNT(chair_sent_at) < 6
		)
		LIMIT 1
	`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 椅子をリクエストに割り当てる
	if _, err := db.ExecContext(ctx, `
		UPDATE rides
		SET chair_id = ?
		WHERE id = ?
	`, matched.ID, ride.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

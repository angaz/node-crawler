package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Migrate006MessageTooBigError(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			ALTER TYPE crawler.error ADD VALUE 'message too big';
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

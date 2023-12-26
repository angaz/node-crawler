package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func ExecutionCapabilitiesUpsert(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE OR REPLACE FUNCTION execution.upsert_capabilities(
				capabilities	TEXT
			) RETURNS INTEGER AS $$
				#variable_conflict use_column

				DECLARE
					local_capabilities	TEXT := capabilities;
				BEGIN
					IF local_capabilities IS NULL THEN
						RETURN NULL;
					END IF;

					INSERT INTO execution.capabilities (
						capabilities
					)
					VALUES (
						local_capabilities
					)
					ON CONFLICT (capabilities) DO NOTHING;

					RETURN capabilities_id
					FROM execution.capabilities
					WHERE capabilities = local_capabilities;
				END
			$$ LANGUAGE plpgsql
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func ClientUpsertStrings(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE OR REPLACE FUNCTION client.upsert(
				client_identifier	TEXT DEFAULT NULL,
				client_name			TEXT DEFAULT NULL,
				client_user_data	TEXT DEFAULT NULL,
				client_version		TEXT DEFAULT NULL,
				client_build		TEXT DEFAULT NULL,
				client_language		TEXT DEFAULT NULL
			) RETURNS TABLE (
				client_identifier_id	INTEGER,
				client_name_id			INTEGER,
				client_user_data_id		INTEGER,
				client_version_id		INTEGER,
				client_build_id			INTEGER,
				client_language_id		INTEGER
			) AS $$
				#variable_conflict use_column

				DECLARE
					local_client_identifier		TEXT := client_identifier;
					local_client_name			TEXT := client_name;
					local_client_user_data		TEXT := client_user_data;
					local_client_version		TEXT := client_version;
					local_client_build			TEXT := client_build;
					local_client_language		TEXT := client_language;

					out_client_identifier_id	INTEGER;
					out_client_name_id			INTEGER;
					out_client_user_data_id		INTEGER;
					out_client_version_id		INTEGER;
					out_client_build_id			INTEGER;
					out_client_language_id		INTEGER;
				BEGIN
					IF local_client_identifier IS NOT NULL THEN
						INSERT INTO client.identifiers (
							client_identifier
						)
						VALUES (
							local_client_identifier
						)
						ON CONFLICT (client_identifier) DO NOTHING;

						SELECT client_identifier_id
						INTO out_client_identifier_id
						FROM client.identifiers
						WHERE client_identifier = local_client_identifier;
					END IF;

					IF local_client_name IS NOT NULL THEN
						INSERT INTO client.names (
							client_name
						)
						VALUES (
							local_client_name
						)
						ON CONFLICT (client_name) DO NOTHING;

						SELECT client_name_id
						INTO out_client_name_id
						FROM client.names
						WHERE client_name = local_client_name;
					END IF;

					IF local_client_user_data IS NOT NULL THEN
						INSERT INTO client.user_data (
							client_user_data
						)
						VALUES (
							local_client_user_data
						)
						ON CONFLICT (client_user_data) DO NOTHING;

						SELECT client_user_data_id
						INTO out_client_user_data_id
						FROM client.user_data
						WHERE client_user_data = local_client_user_data;
					END IF;

					IF local_client_version IS NOT NULL THEN
						INSERT INTO client.versions (
							client_version
						)
						VALUES (
							local_client_version
						)
						ON CONFLICT (client_version) DO NOTHING;

						SELECT client_version_id
						INTO out_client_version_id
						FROM client.versions
						WHERE client_version = local_client_version;
					END IF;

					IF local_client_build IS NOT NULL THEN
						INSERT INTO client.builds (
							client_build
						)
						VALUES (
							local_client_build
						)
						ON CONFLICT (client_build) DO NOTHING;

						SELECT client_build_id
						INTO out_client_build_id
						FROM client.builds
						WHERE client_build = local_client_build;
					END IF;

					IF local_client_language IS NOT NULL THEN
						INSERT INTO client.languages (
							client_language
						)
						VALUES (
							local_client_language
						)
						ON CONFLICT (client_language) DO NOTHING;

						SELECT client_language_id
						INTO out_client_language_id
						FROM client.languages
						WHERE client_language = local_client_language;
					END IF;

					RETURN QUERY SELECT
						out_client_identifier_id	client_identifier_id,
						out_client_name_id			client_name_id,
						out_client_user_data_id		client_user_data_id,
						out_client_version_id		client_version_id,
						out_client_build_id			client_build_id,
						out_client_language_id		client_language_id;
				END
			$$ LANGUAGE plpgsql
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

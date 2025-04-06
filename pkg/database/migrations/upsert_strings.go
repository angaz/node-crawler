package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func RandomSeconds(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE OR REPLACE FUNCTION random_seconds(max_seconds INTEGER) RETURNS INTERVAL
			AS 'SELECT make_interval(secs => random() * max_seconds);'
			IMMUTABLE
			LANGUAGE SQL
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

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
					out_capabilities_id	INTEGER;
				BEGIN
					IF local_capabilities IS NOT NULL THEN
						SELECT capabilities_id
						INTO out_capabilities_id
						FROM execution.capabilities
						WHERE capabilities = local_capabilities;

						IF out_capabilities_id IS NULL THEN
							INSERT INTO execution.capabilities (
								capabilities
							)
							VALUES (
								local_capabilities
							)
							RETURNING
								capabilities_id INTO out_capabilities_id;
						END IF;
					END IF;

					RETURN out_capabilities_id;
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
				client_identifier	TEXT,
				client_name			TEXT,
				client_user_data	TEXT,
				client_version		TEXT,
				client_build		TEXT,
				client_os			client.os,
				client_arch			client.arch,
				client_language		TEXT
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
					IF local_client_name IS NOT NULL THEN
						SELECT client_name_id
						INTO out_client_name_id
						FROM client.names
						WHERE client_name = local_client_name;

						IF out_client_name_id IS NULL THEN
							INSERT INTO client.names (
								client_name
							)
							VALUES (
								local_client_name
							)
							RETURNING
								client_name_id INTO out_client_name_id;
						END IF;
					END IF;

					IF local_client_user_data IS NOT NULL THEN
						SELECT client_user_data_id
						INTO out_client_user_data_id
						FROM client.user_data
						WHERE client_user_data = local_client_user_data;

						IF out_client_user_data_id IS NULL THEN
							INSERT INTO client.user_data (
								client_user_data
							)
							VALUES (
								local_client_user_data
							)
							RETURNING
								client_user_data_id INTO out_client_user_data_id;
						END IF;
					END IF;

					IF local_client_version IS NOT NULL THEN
						SELECT client_version_id
						INTO out_client_version_id
						FROM client.versions
						WHERE client_version = local_client_version;

						IF out_client_version_id IS NULL THEN
							INSERT INTO client.versions (
								client_version
							)
							VALUES (
								local_client_version
							)
							RETURNING
								client_version_id INTO out_client_version_id;
						END IF;
					END IF;

					IF local_client_build IS NOT NULL THEN
						SELECT client_build_id
						INTO out_client_build_id
						FROM client.builds
						WHERE client_build = local_client_build;

						IF out_client_build_id IS NULL THEN
							INSERT INTO client.builds (
								client_build
							)
							VALUES (
								local_client_build
							)
							RETURNING
								client_build_id INTO out_client_build_id;
						END IF;
					END IF;

					IF local_client_language IS NOT NULL THEN
						SELECT client_language_id
						INTO out_client_language_id
						FROM client.languages
						WHERE client_language = local_client_language;

						IF out_client_language_id IS NULL THEN
							INSERT INTO client.languages (
								client_language
							)
							VALUES (
								local_client_language
							)
							RETURNING
								client_language_id INTO out_client_language_id;
						END IF;
					END IF;

					IF local_client_identifier IS NOT NULL THEN
						SELECT client_identifier_id
						INTO out_client_identifier_id
						FROM client.identifiers
						WHERE client_identifier = local_client_identifier;

						IF out_client_identifier_id IS NULL THEN
							INSERT INTO client.identifiers (
								client_identifier,
								client_name_id,
								client_user_data_id,
								client_version_id,
								client_build_id,
								client_os,
								client_arch,
								client_language_id
							)
							VALUES (
								local_client_identifier,
								out_client_name_id,
								out_client_user_data_id,
								out_client_version_id,
								out_client_build_id,
								client_os,
								client_arch,
								out_client_language_id
							)
							RETURNING
								client_identifier_id INTO out_client_identifier_id;
						END IF;
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

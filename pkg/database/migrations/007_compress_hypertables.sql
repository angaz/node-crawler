ALTER TABLE stats.execution_nodes SET (
    timescaledb.compress = true,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'network_id, synced, next_fork_id, client_name_id'
);

CALL add_columnstore_policy('stats.execution_nodes', after => INTERVAL '30d');

SELECT add_retention_policy('crawler.history', INTERVAL '30 days');

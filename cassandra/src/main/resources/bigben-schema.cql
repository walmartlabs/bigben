-- DROP KEYSPACE IF EXISTS bigben;

CREATE KEYSPACE IF NOT EXISTS bigben WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

-- DROP TABLE IF EXISTS bigben.buckets;

CREATE TABLE IF NOT EXISTS bigben.buckets (
	id timestamp PRIMARY KEY,
	count bigint,
	failed_shards text,
	modified_at timestamp,
	processed_at timestamp,
	status text
);

-- DROP TABLE IF EXISTS bigben.lookups;

CREATE TABLE IF NOT EXISTS bigben.lookups (
	tenant text,
	xref_id text,
	bucket_id timestamp,
	event_id text,
	event_time timestamp,
	l_m timestamp,
	payload text,
	shard int,
	PRIMARY KEY ((tenant, xref_id))
);

-- DROP TABLE IF EXISTS bigben.events;

CREATE TABLE IF NOT EXISTS bigben.events (
	bucket_id timestamp,
	shard int,
	event_time timestamp,
	id text,
	error text,
	payload text,
	processed_at timestamp,
	status text,
	tenant text,
	xref_id text,
	PRIMARY KEY ((bucket_id, shard), event_time, id)
) WITH CLUSTERING ORDER BY (event_time ASC, id ASC);

-- DROP TABLE IF EXISTS bigben.kv_table;

CREATE TABLE IF NOT EXISTS bigben.kv_table (
	key text,
	column text,
	l_m timestamp,
	value text,
	PRIMARY KEY (key, column)
) WITH CLUSTERING ORDER BY (column ASC);


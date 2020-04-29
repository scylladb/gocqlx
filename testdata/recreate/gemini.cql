CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TYPE ks1.udt_17622148 (
    udt_17622148_0 text
);

CREATE TABLE ks1.table1 (
    pk0 tinyint,
    pk1 int,
    pk2 varint,
    pk3 int,
    ck0 decimal,
    ck1 ascii,
    col0 frozen<udt_17622148>,
    col1 date,
    col10 float,
    col2 boolean,
    col3 varint,
    col4 frozen<tuple<blob, int>>,
    col5 tinyint,
    col6 timeuuid,
    col7 timeuuid,
    col8 timestamp,
    col9 date,
    PRIMARY KEY ((pk0, pk1, pk2, pk3), ck0, ck1)
) WITH CLUSTERING ORDER BY (ck0 ASC, ck1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

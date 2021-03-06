SELECT count(*) AS columnar_table_count FROM cstore.cstore_data_files \gset

CREATE TABLE t(a int, b int) USING columnar;

SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

INSERT INTO t SELECT i, i * i FROM generate_series(1, 10) i;
INSERT INTO t SELECT i, i * i FROM generate_series(11, 20) i;
INSERT INTO t SELECT i, i * i FROM generate_series(21, 30) i;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

-- vacuum full should merge stripes together
VACUUM FULL t;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

-- test the case when all data cannot fit into a single stripe
SELECT alter_columnar_table_set('t', stripe_row_count => 1000);
INSERT INTO t SELECT i, 2 * i FROM generate_series(1,2500) i;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

VACUUM FULL t;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

-- VACUUM FULL doesn't reclaim dropped columns, but converts them to NULLs
ALTER TABLE t DROP COLUMN a;

SELECT stripe, attr, block, minimum_value IS NULL, maximum_value IS NULL FROM cstore.cstore_skipnodes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t' ORDER BY 1, 2, 3;

VACUUM FULL t;

SELECT stripe, attr, block, minimum_value IS NULL, maximum_value IS NULL FROM cstore.cstore_skipnodes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t' ORDER BY 1, 2, 3;

-- Make sure we cleaned-up the transient table metadata after VACUUM FULL commands
SELECT count(*) - :columnar_table_count FROM cstore.cstore_data_files;

-- do this in a transaction so concurrent autovacuum doesn't interfere with results
BEGIN;
SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT pg_size_pretty(pg_relation_size('t'));
INSERT INTO t SELECT i FROM generate_series(1, 10000) i;
SELECT pg_size_pretty(pg_relation_size('t'));
SELECT count(*) FROM t;
ROLLBACK TO SAVEPOINT s1;

-- not truncated by VACUUM or autovacuum yet (being in transaction ensures this),
-- so relation size should be same as before.
SELECT pg_size_pretty(pg_relation_size('t'));
COMMIT;

-- vacuum should truncate the relation to the usable space
VACUUM VERBOSE t;
SELECT pg_size_pretty(pg_relation_size('t'));
SELECT count(*) FROM t;

-- add some stripes with different compression types and create some gaps,
-- then vacuum to print stats

BEGIN;
SELECT alter_columnar_table_set('t',
    block_row_count => 1000,
    stripe_row_count => 2000,
    compression => 'pglz');
SAVEPOINT s1;
INSERT INTO t SELECT i FROM generate_series(1, 1500) i;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t SELECT i / 5 FROM generate_series(1, 1500) i;
SELECT alter_columnar_table_set('t', compression => 'none');
SAVEPOINT s2;
INSERT INTO t SELECT i FROM generate_series(1, 1500) i;
ROLLBACK TO SAVEPOINT s2;
INSERT INTO t SELECT i / 5 FROM generate_series(1, 1500) i;
COMMIT;

VACUUM VERBOSE t;

SELECT count(*) FROM t;

-- check that we report blocks with data for dropped columns
ALTER TABLE t ADD COLUMN c int;
INSERT INTO t SELECT 1, i / 5 FROM generate_series(1, 1500) i;
ALTER TABLE t DROP COLUMN c;

VACUUM VERBOSE t;

-- vacuum full should remove blocks for dropped columns
-- note that, a block will be stored in non-compressed for if compression
-- doesn't reduce its size.
SELECT alter_columnar_table_set('t', compression => 'pglz');
VACUUM FULL t;
VACUUM VERBOSE t;

DROP TABLE t;

-- Make sure we cleaned the metadata for t too
SELECT count(*) - :columnar_table_count FROM cstore.cstore_data_files;

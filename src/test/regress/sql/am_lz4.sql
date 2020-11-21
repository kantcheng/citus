--
-- Testing insert on cstore_fdw tables.
--

SET cstore.compression TO 'lz4';
CREATE TABLE test_lz4 (a int, b text) USING columnar;

CREATE VIEW test_lz4_block_stats AS
	SELECT stripe, attr, row_count, value_compression_type,
		floor((100.0 * value_stream_length)/value_uncompressed_length) AS compression_ratio
	FROM cstore.cstore_skipnodes a, pg_class b
	WHERE a.relfilenode = b.relfilenode AND b.relname='test_lz4'
	ORDER BY 1, 2;

INSERT INTO test_lz4 SELECT i, floor(i / 10)::text FROM generate_series(1, 100) i;
SELECT count(*) FROM test_lz4;

INSERT INTO test_lz4 SELECT i, floor(i / 10)::text FROM generate_series(1000, 1100) i;
SELECT count(*) FROM test_lz4;

SELECT * FROM test_lz4_block_stats;

SELECT DISTINCT * FROM test_lz4 ORDER BY a, b LIMIT 5;

DROP TABLE test_lz4 CASCADE;

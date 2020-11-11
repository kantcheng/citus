-- citus--9.5-1--10.0-1

-- bump version to 10.0-1

#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

CREATE VIEW public.citus_tables AS
SELECT
  logicalrelid AS table_name,
  CASE WHEN partkey IS NOT NULL THEN 'distributed' WHEN repmodel = 't' THEN 'reference' ELSE 'local' END AS table_type,
  column_to_column_name(logicalrelid, partkey) AS distribution_column,
  pg_size_pretty(citus_total_relation_size(logicalrelid)) AS table_size,
  (select count(*) from pg_dist_shard where logicalrelid = p.logicalrelid) AS shard_count,
  amname AS access_method
FROM
  pg_dist_partition p
JOIN
  pg_class c ON (p.logicalrelid = c.oid)
LEFT JOIN
  pg_am a ON (a.oid = c.relam)
ORDER BY
  logicalrelid::text;

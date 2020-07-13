/*-------------------------------------------------------------------------
 *
 * shard_utils.h
 *   Utilities related to shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_UTILS_H
#define SHARD_UTILS_H

#include "postgres.h"

extern Oid GetTableLocalShardOid(Oid citusTableOid, uint64 shardId);
extern Oid GetNoneDistTableLocalShardRelationId(Oid noneDistTableId);
extern uint64 GetNoneDistTableShardId(Oid noneDistTableId);
extern void AppendNoneDistTableShardIdToName(Oid noneDistTableId, char **name);

#endif /* SHARD_UTILS_H */

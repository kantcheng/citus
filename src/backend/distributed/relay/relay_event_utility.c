/*-------------------------------------------------------------------------
 *
 * relay_event_utility.c
 *
 * Routines for handling DDL statements that relate to relay files. These
 * routines extend relation, index and constraint names in utility commands.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stdio.h>
#include <string.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/hash.h"
#include "access/htup.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "columnar/cstore_tableam.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/relay_utility.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/regproc.h"
#include "utils/relcache.h"

/* Local functions forward declarations */
static bool UpdateWholeRowColumnReferencesWalker(Node *node, uint64 *shardId);
static bool IsCstoreAlterTableOptionsFunctionCall(SelectStmt *stmt);
static void RewriteCStoreAlterTableOptionsFunctionCallForShard(SelectStmt *stmt,
															   char *schemaName,
															   uint64 shardId);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(shard_name);

/*
 * RelayEventExtendNames extends relation names in the given parse tree for
 * certain utility commands. The function more specifically extends table and
 * index names in the parse tree by appending the given shardId; thereby
 * avoiding name collisions in the database among sharded tables. This function
 * has the side effect of extending relation names in the parse tree.
 */
void
RelayEventExtendNames(Node *parseTree, char *schemaName, uint64 shardId)
{
	/* we don't extend names in extension or schema commands */
	NodeTag nodeType = nodeTag(parseTree);
	if (nodeType == T_CreateExtensionStmt || nodeType == T_CreateSchemaStmt ||
		nodeType == T_CreateSeqStmt || nodeType == T_AlterSeqStmt)
	{
		return;
	}

	switch (nodeType)
	{
		case T_AlterObjectSchemaStmt:
		{
			AlterObjectSchemaStmt *alterObjectSchemaStmt =
				(AlterObjectSchemaStmt *) parseTree;
			char **relationName = &(alterObjectSchemaStmt->relation->relname);
			char **relationSchemaName = &(alterObjectSchemaStmt->relation->schemaname);

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			/* append shardId to base relation name */
			AppendShardIdToName(relationName, shardId);
			break;
		}

		case T_AlterTableStmt:
		{
			/*
			 * We append shardId to the very end of table and index, constraint
			 * and trigger names to avoid name collisions.
			 */

			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parseTree;
			Oid relationId = InvalidOid;
			char **relationName = &(alterTableStmt->relation->relname);
			char **relationSchemaName = &(alterTableStmt->relation->schemaname);

			List *commandList = alterTableStmt->cmds;

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			/* append shardId to base relation name */
			AppendShardIdToName(relationName, shardId);

			AlterTableCmd *command = NULL;
			foreach_ptr(command, commandList)
			{
				if (command->subtype == AT_AddConstraint)
				{
					Constraint *constraint = (Constraint *) command->def;
					char **constraintName = &(constraint->conname);

					if (constraint->contype == CONSTR_PRIMARY && constraint->indexname)
					{
						char **indexName = &(constraint->indexname);
						AppendShardIdToName(indexName, shardId);
					}

					AppendShardIdToName(constraintName, shardId);
				}
				else if (command->subtype == AT_DropConstraint ||
						 command->subtype == AT_ValidateConstraint)
				{
					char **constraintName = &(command->name);
					const bool constraintMissingOk = true;

					if (!OidIsValid(relationId))
					{
						const bool rvMissingOk = false;
						relationId = RangeVarGetRelid(alterTableStmt->relation,
													  AccessShareLock,
													  rvMissingOk);
					}

					Oid constraintOid = get_relation_constraint_oid(relationId,
																	command->name,
																	constraintMissingOk);
					if (!OidIsValid(constraintOid))
					{
						AppendShardIdToName(constraintName, shardId);
					}
				}
				else if (command->subtype == AT_ClusterOn)
				{
					char **indexName = &(command->name);
					AppendShardIdToName(indexName, shardId);
				}
				else if (command->subtype == AT_ReplicaIdentity)
				{
					ReplicaIdentityStmt *replicaIdentity =
						(ReplicaIdentityStmt *) command->def;

					if (replicaIdentity->identity_type == REPLICA_IDENTITY_INDEX)
					{
						char **indexName = &(replicaIdentity->name);
						AppendShardIdToName(indexName, shardId);
					}
				}
				else if (command->subtype == AT_EnableTrig ||
						 command->subtype == AT_DisableTrig ||
						 command->subtype == AT_EnableAlwaysTrig ||
						 command->subtype == AT_EnableReplicaTrig)
				{
					char **triggerName = &(command->name);
					AppendShardIdToName(triggerName, shardId);
				}
			}

			break;
		}

		case T_ClusterStmt:
		{
			ClusterStmt *clusterStmt = (ClusterStmt *) parseTree;

			/* we do not support clustering the entire database */
			if (clusterStmt->relation == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for multi-relation cluster")));
			}

			char **relationName = &(clusterStmt->relation->relname);
			char **relationSchemaName = &(clusterStmt->relation->schemaname);

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			AppendShardIdToName(relationName, shardId);

			if (clusterStmt->indexname != NULL)
			{
				char **indexName = &(clusterStmt->indexname);
				AppendShardIdToName(indexName, shardId);
			}

			break;
		}

		case T_CreateForeignServerStmt:
		{
			CreateForeignServerStmt *serverStmt = (CreateForeignServerStmt *) parseTree;
			char **serverName = &(serverStmt->servername);

			AppendShardIdToName(serverName, shardId);
			break;
		}

		case T_CreateForeignTableStmt:
		{
			CreateForeignTableStmt *createStmt = (CreateForeignTableStmt *) parseTree;
			char **serverName = &(createStmt->servername);

			AppendShardIdToName(serverName, shardId);

			/*
			 * Since CreateForeignTableStmt inherits from CreateStmt and any change
			 * performed on CreateStmt should be done here too, we simply *fall
			 * through* to avoid code repetition.
			 */
		}

		/* fallthrough */
		case T_CreateStmt:
		{
			CreateStmt *createStmt = (CreateStmt *) parseTree;
			char **relationName = &(createStmt->relation->relname);
			char **relationSchemaName = &(createStmt->relation->schemaname);

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			AppendShardIdToName(relationName, shardId);
			break;
		}

		case T_CreateTrigStmt:
		{
			CreateTrigStmt *createTriggerStmt = (CreateTrigStmt *) parseTree;
			CreateTriggerEventExtendNames(createTriggerStmt, schemaName, shardId);
			break;
		}

		case T_AlterObjectDependsStmt:
		{
			AlterObjectDependsStmt *alterTriggerDependsStmt =
				(AlterObjectDependsStmt *) parseTree;
			ObjectType objectType = alterTriggerDependsStmt->objectType;

			if (objectType == OBJECT_TRIGGER)
			{
				AlterTriggerDependsEventExtendNames(alterTriggerDependsStmt,
													schemaName, shardId);
			}
			else
			{
				ereport(WARNING, (errmsg("unsafe object type in alter object "
										 "depends statement"),
								  errdetail("Object type: %u", (uint32) objectType)));
			}
			break;
		}

		case T_DropStmt:
		{
			DropStmt *dropStmt = (DropStmt *) parseTree;
			ObjectType objectType = dropStmt->removeType;

			if (objectType == OBJECT_TABLE || objectType == OBJECT_INDEX ||
				objectType == OBJECT_FOREIGN_TABLE || objectType == OBJECT_FOREIGN_SERVER)
			{
				Value *relationSchemaNameValue = NULL;
				Value *relationNameValue = NULL;

				uint32 dropCount = list_length(dropStmt->objects);
				if (dropCount > 1)
				{
					ereport(ERROR,
							(errmsg("cannot extend name for multiple drop objects")));
				}

				/*
				 * We now need to extend a single relation or index name. To be
				 * able to do this extension, we need to extract the names'
				 * addresses from the value objects they are stored in. Other-
				 * wise, the repalloc called in AppendShardIdToName() will not
				 * have the correct memory address for the name.
				 */

				List *relationNameList = (List *) linitial(dropStmt->objects);
				int relationNameListLength = list_length(relationNameList);

				switch (relationNameListLength)
				{
					case 1:
					{
						relationNameValue = linitial(relationNameList);
						break;
					}

					case 2:
					{
						relationSchemaNameValue = linitial(relationNameList);
						relationNameValue = lsecond(relationNameList);
						break;
					}

					case 3:
					{
						relationSchemaNameValue = lsecond(relationNameList);
						relationNameValue = lthird(relationNameList);
						break;
					}

					default:
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("improper relation name: \"%s\"",
											   NameListToString(relationNameList))));
						break;
					}
				}

				/* prefix with schema name if it is not added already */
				if (relationSchemaNameValue == NULL)
				{
					Value *schemaNameValue = makeString(pstrdup(schemaName));
					relationNameList = lcons(schemaNameValue, relationNameList);
				}

				char **relationName = &(relationNameValue->val.str);
				AppendShardIdToName(relationName, shardId);
			}
			else if (objectType == OBJECT_POLICY)
			{
				DropPolicyEventExtendNames(dropStmt, schemaName, shardId);
			}
			else if (objectType == OBJECT_TRIGGER)
			{
				DropTriggerEventExtendNames(dropStmt, schemaName, shardId);
			}
			else
			{
				ereport(WARNING, (errmsg("unsafe object type in drop statement"),
								  errdetail("Object type: %u", (uint32) objectType)));
			}

			break;
		}


		case T_GrantStmt:
		{
			GrantStmt *grantStmt = (GrantStmt *) parseTree;
			if (grantStmt->targtype == ACL_TARGET_OBJECT &&
				grantStmt->objtype == OBJECT_TABLE)
			{
				RangeVar *relation = NULL;
				foreach_ptr(relation, grantStmt->objects)
				{
					char **relationName = &(relation->relname);
					char **relationSchemaName = &(relation->schemaname);

					/* prefix with schema name if it is not added already */
					SetSchemaNameIfNotExist(relationSchemaName, schemaName);

					AppendShardIdToName(relationName, shardId);
				}
			}
			break;
		}

		case T_CreatePolicyStmt:
		{
			CreatePolicyEventExtendNames((CreatePolicyStmt *) parseTree, schemaName,
										 shardId);
			break;
		}

		case T_AlterPolicyStmt:
		{
			AlterPolicyEventExtendNames((AlterPolicyStmt *) parseTree, schemaName,
										shardId);
			break;
		}

		case T_IndexStmt:
		{
			IndexStmt *indexStmt = (IndexStmt *) parseTree;
			char **relationName = &(indexStmt->relation->relname);
			char **indexName = &(indexStmt->idxname);
			char **relationSchemaName = &(indexStmt->relation->schemaname);

			/*
			 * Concurrent index statements cannot run within a transaction block.
			 * Therefore, we do not support them.
			 */
			if (indexStmt->concurrent)
			{
				ereport(ERROR, (errmsg("cannot extend name for concurrent index")));
			}

			/*
			 * In the regular DDL execution code path (for non-sharded tables),
			 * if the index statement results from a table creation command, the
			 * indexName may be null. For sharded tables however, we intercept
			 * that code path and explicitly set the index name. Therefore, the
			 * index name in here cannot be null.
			 */
			if ((*indexName) == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for null index name")));
			}

			/* extend ColumnRef nodes in the IndexStmt with the shardId */
			UpdateWholeRowColumnReferencesWalker((Node *) indexStmt->indexParams,
												 &shardId);

			/* prefix with schema name if it is not added already */
			SetSchemaNameIfNotExist(relationSchemaName, schemaName);

			AppendShardIdToName(relationName, shardId);
			AppendShardIdToName(indexName, shardId);
			break;
		}

		case T_ReindexStmt:
		{
			ReindexStmt *reindexStmt = (ReindexStmt *) parseTree;

			ReindexObjectType objectType = reindexStmt->kind;
			if (objectType == REINDEX_OBJECT_TABLE || objectType == REINDEX_OBJECT_INDEX)
			{
				char **objectName = &(reindexStmt->relation->relname);
				char **objectSchemaName = &(reindexStmt->relation->schemaname);

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(objectSchemaName, schemaName);

				AppendShardIdToName(objectName, shardId);
			}

			break;
		}

		case T_RenameStmt:
		{
			RenameStmt *renameStmt = (RenameStmt *) parseTree;
			ObjectType objectType = renameStmt->renameType;

			if (objectType == OBJECT_TABLE || objectType == OBJECT_INDEX ||
				objectType == OBJECT_FOREIGN_TABLE)
			{
				char **oldRelationName = &(renameStmt->relation->relname);
				char **newRelationName = &(renameStmt->newname);
				char **objectSchemaName = &(renameStmt->relation->schemaname);

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(objectSchemaName, schemaName);

				AppendShardIdToName(oldRelationName, shardId);
				AppendShardIdToName(newRelationName, shardId);

				/*
				 * PostgreSQL creates array types for each ordinary table, with
				 * the same name plus a prefix of '_'.
				 *
				 * ALTER TABLE ... RENAME TO ... also renames the underlying
				 * array type, and the DDL is run in parallel connections over
				 * all the placements and shards at once. Concurrent access
				 * here deadlocks.
				 *
				 * Let's provide an easier to understand error message here
				 * than the deadlock one.
				 *
				 * See also https://github.com/citusdata/citus/issues/1664
				 */
				int newRelationNameLength = strlen(*newRelationName);
				if (newRelationNameLength >= (NAMEDATALEN - 1))
				{
					ereport(ERROR,
							(errcode(ERRCODE_NAME_TOO_LONG),
							 errmsg(
								 "shard name %s exceeds %d characters",
								 *newRelationName, NAMEDATALEN - 1)));
				}
			}
			else if (objectType == OBJECT_COLUMN)
			{
				char **relationName = &(renameStmt->relation->relname);
				char **objectSchemaName = &(renameStmt->relation->schemaname);

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(objectSchemaName, schemaName);

				AppendShardIdToName(relationName, shardId);
			}
			else if (objectType == OBJECT_TRIGGER)
			{
				AlterTriggerRenameEventExtendNames(renameStmt, schemaName, shardId);
			}
			else if (objectType == OBJECT_POLICY)
			{
				RenamePolicyEventExtendNames(renameStmt, schemaName, shardId);
			}
			else
			{
				ereport(WARNING, (errmsg("unsafe object type in rename statement"),
								  errdetail("Object type: %u", (uint32) objectType)));
			}

			break;
		}

		case T_TruncateStmt:
		{
			/*
			 * We currently do not support truncate statements. This is
			 * primarily because truncates allow implicit modifications to
			 * sequences through table column dependencies. As we have not
			 * determined our dependency model for sequences, we error here.
			 */
			ereport(ERROR, (errmsg("cannot extend name for truncate statement")));
			break;
		}

		case T_SelectStmt:
		{
			/*
			 * Since postgres doesn't allow custom options on a table access method table
			 * we use a UDF to apply the options to the table. This function effectively
			 * behaves like a ddl command.
			 *
			 * Since the coordinator outsourced the application of the shard id in the ddl
			 * commands to the worker we use the same infrastructure to route this UDF
			 * through and apply the shard name.
			 *
			 * Execution of the UDF will be deferred to a later point.
			 */
			if (IsCstoreAlterTableOptionsFunctionCall(castNode(SelectStmt, parseTree)))
			{
				SelectStmt *stmt = castNode(SelectStmt, parseTree);
				RewriteCStoreAlterTableOptionsFunctionCallForShard(stmt, schemaName,
																   shardId);
			}
			else
			{
				ereport(WARNING, (errmsg("unsafe statement type in name extension"),
								  errdetail("Statement type: %u", (uint32) nodeType)));
			}
			break;
		}

		default:
		{
			ereport(WARNING, (errmsg("unsafe statement type in name extension"),
							  errdetail("Statement type: %u", (uint32) nodeType)));
			break;
		}
	}
}


static FuncCall *
ExtractCStoreAtlterTableOptionsFunctionCall(SelectStmt *stmt)
{
	if (stmt == NULL)
	{
		return NULL;
	}

	/* cstore options have only 1 function in the target list */
	if (list_length(stmt->targetList) != 1)
	{
		return NULL;
	}

	Node *targetNode = (Node *) linitial(stmt->targetList);
	if (targetNode == NULL || !IsA(targetNode, ResTarget))
	{
		return NULL;
	}

	ResTarget *target = castNode(ResTarget, linitial(stmt->targetList));
	if (target->val == NULL || !IsA(target->val, FuncCall))
	{
		return NULL;
	}

	FuncCall *funcCall = castNode(FuncCall, target->val);
	if (list_length(funcCall->funcname) != 1)
	{
		return NULL;
	}

	Node *funcNameNode = linitial(funcCall->funcname);
	if (funcNameNode == NULL || !IsA(funcNameNode, String))
	{
		return NULL;
	}

	char *funcName = strVal(funcNameNode);
	if (strncmp(funcName, "alter_cstore_table_set", NAMEDATALEN) != 0)
	{
		return NULL;
	}


	/* TODO, verify it has the table_name argument set */

	return funcCall;
}


static bool
IsCstoreAlterTableOptionsFunctionCall(SelectStmt *stmt)
{
	FuncCall *funcCall = ExtractCStoreAtlterTableOptionsFunctionCall(stmt);
	if (funcCall == NULL)
	{
		return false;
	}

	return true;
}


static void
RewriteCStoreAlterTableOptionsFunctionCallForShard(SelectStmt *stmt, char *schemaName,
												   uint64 shardId)
{
	FuncCall *funcCall = ExtractCStoreAtlterTableOptionsFunctionCall(stmt);

	/* We have already checked we have a func call, assert instead of check */
	Assert(funcCall != NULL);

	if (list_length(funcCall->args) < 1)
	{
		/* no first argument, nothing to update */
		return;
	}

	Node *tableNameArgument = linitial(funcCall->args);
	if (!IsA(tableNameArgument, A_Const))
	{
		/* first argument is not a constant, no reason to apply shardid if not constant */
		return;
	}

	A_Const *tableNameConst = castNode(A_Const, tableNameArgument);
	if (!IsA(&tableNameConst->val, String))
	{
		/* not a string constant, probablyb not a valid name to add the shard id to */
		return;
	}

	/* here we know we are mutating the table name argument */

	Value *tableNameString = &tableNameConst->val;
	char *originalTableName = strVal(tableNameString);
	List *originalTableNameList = stringToQualifiedNameList(originalTableName);

	RangeVar *rangeVar = makeRangeVarFromNameList(originalTableNameList);

	/* finally we have the relation name to append the shardid to, inplace */
	AppendShardIdToName(&rangeVar->relname, shardId);

	List *newTableNameList = MakeNameListFromRangeVar(rangeVar);
	char *newTableName = NameListToQuotedString(newTableNameList);

	/* allocate the string in the same namespace as the constant */
	MemoryContext constMemoryContext = GetMemoryChunkContext(tableNameConst);
	newTableName = MemoryContextStrdup(constMemoryContext, newTableName);

	/* replace the original pointer in the constant */
	tableNameString->val.str = newTableName;
}


/*
 * ExecuteCitusDDLFunction has a small list of allowed functions that can be called. For
 * security and simplicity reasons we will not pass them through the postgres planner and
 * executor, instead we implement the execution of these functions inline, parsing their
 * arguments as we expect them or throw an error.
 */
void
ExecuteCitusDDLFunction(SelectStmt *stmt)
{
	if (IsCstoreAlterTableOptionsFunctionCall(stmt))
	{
		/* alter_cstore_table_set function call*/
		FuncCall *funcCall = ExtractCStoreAtlterTableOptionsFunctionCall(stmt);

		Assert(funcCall != NULL);

		if (list_length(funcCall->args) <= 0)
		{
			ereport(ERROR, (errmsg("missing arguments to citus ddl function call")));
		}

		Node *tableNameNode = linitial(funcCall->args);
		if (!IsA(tableNameNode, A_Const) ||
			!IsA(&castNode(A_Const, tableNameNode)->val, String))
		{
			ereport(ERROR, (errmsg("unsupported arguments to citus ddl function call")));
		}

		/* direct function call, but we have NULL values to set, yay! */
		LOCAL_FCINFO(fcinfo, 4);

		InitFunctionCallInfoData(*fcinfo, NULL, 4, InvalidOid, NULL, NULL);

		/* table_name */
		char *tableName = strVal(&castNode(A_Const, tableNameNode)->val);
		List *tableNameList = stringToQualifiedNameList(tableName);
		RangeVar *rangeVar = makeRangeVarFromNameList(tableNameList);
		Oid tableOid = RangeVarGetRelid(rangeVar, AccessShareLock, false);
		fcSetArg(fcinfo, 0, ObjectIdGetDatum(tableOid));

		/* preload all arguments with NULL */
		fcSetArgNull(fcinfo, 1);
		fcSetArgNull(fcinfo, 2);
		fcSetArgNull(fcinfo, 3);

		Node *argNode = NULL;
		bool first = true;
		foreach_ptr(argNode, funcCall->args)
		{
			if (first)
			{
				/* skip parsing of the first argument as we have already parsed it */
				first = false;
				continue;
			}

			if (!IsA(argNode, NamedArgExpr))
			{
				ereport(ERROR, (errmsg(
									"unsupported arguments to citus ddl function call")));
			}

			NamedArgExpr *arg = castNode(NamedArgExpr, argNode);
			if (strncmp(arg->name, "block_row_count", 15) == 0)
			{
				if (!IsA(arg->arg, A_Const) ||
					!IsA(&castNode(A_Const, arg->arg)->val, Integer))
				{
					ereport(ERROR, (errmsg(
										"unsupported arguments to citus ddl function call")));
				}

				int block_row_count = intVal(&castNode(A_Const, arg->arg)->val);

				fcSetArg(fcinfo, 1, Int32GetDatum(block_row_count));
			}
			else if (strncmp(arg->name, "stripe_row_count", 16) == 0)
			{
				if (!IsA(arg->arg, A_Const) ||
					!IsA(&castNode(A_Const, arg->arg)->val, Integer))
				{
					ereport(ERROR, (errmsg(
										"unsupported arguments to citus ddl function call")));
				}

				int stripe_row_count = intVal(&castNode(A_Const, arg->arg)->val);

				fcSetArg(fcinfo, 2, Int32GetDatum(stripe_row_count));
			}
			else if (strncmp(arg->name, "compression", 11) == 0)
			{
				if (!IsA(arg->arg, A_Const) ||
					!IsA(&castNode(A_Const, arg->arg)->val, String))
				{
					ereport(ERROR, (errmsg(
										"unsupported arguments to citus ddl function call")));
				}

				char *compression = strVal(&castNode(A_Const, arg->arg)->val);
				NameData compressionName = { 0 };

				namestrcpy(&compressionName, compression);

				fcSetArg(fcinfo, 3, NameGetDatum(&compressionName));
			}
			else
			{
				ereport(ERROR, (errmsg(
									"unsupported arguments to citus ddl function call")));
			}
		}

		/* invoke function */
		alter_cstore_table_set(fcinfo);
	}
	else
	{
		ereport(ERROR, (errmsg("unsupported citus ddl function call")));
	}
}


/*
 * RelayEventExtendNamesForInterShardCommands extends relation names in the given parse
 * tree for certain utility commands. The function more specifically extends table and
 * constraint names in the parse tree by appending the given shardId; thereby
 * avoiding name collisions in the database among sharded tables. This function
 * has the side effect of extending relation names in the parse tree.
 */
void
RelayEventExtendNamesForInterShardCommands(Node *parseTree, uint64 leftShardId,
										   char *leftShardSchemaName, uint64 rightShardId,
										   char *rightShardSchemaName)
{
	NodeTag nodeType = nodeTag(parseTree);

	switch (nodeType)
	{
		case T_AlterTableStmt:
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parseTree;
			List *commandList = alterTableStmt->cmds;

			AlterTableCmd *command = NULL;
			foreach_ptr(command, commandList)
			{
				char **referencedTableName = NULL;
				char **relationSchemaName = NULL;

				if (command->subtype == AT_AddConstraint)
				{
					Constraint *constraint = (Constraint *) command->def;
					if (constraint->contype == CONSTR_FOREIGN)
					{
						referencedTableName = &(constraint->pktable->relname);
						relationSchemaName = &(constraint->pktable->schemaname);
					}
				}
				else if (command->subtype == AT_AddColumn)
				{
					/*
					 * TODO: This code path will never be executed since we do not
					 * support foreign constraint creation via
					 * ALTER TABLE %s ADD COLUMN %s [constraint]. However, the code
					 * is kept in case we fix the constraint creation without a name
					 * and allow foreign key creation with the mentioned command.
					 */
					ColumnDef *columnDefinition = (ColumnDef *) command->def;
					List *columnConstraints = columnDefinition->constraints;

					Constraint *constraint = NULL;
					foreach_ptr(constraint, columnConstraints)
					{
						if (constraint->contype == CONSTR_FOREIGN)
						{
							referencedTableName = &(constraint->pktable->relname);
							relationSchemaName = &(constraint->pktable->schemaname);
						}
					}
				}
				else if (command->subtype == AT_AttachPartition ||
						 command->subtype == AT_DetachPartition)
				{
					PartitionCmd *partitionCommand = (PartitionCmd *) command->def;

					referencedTableName = &(partitionCommand->name->relname);
					relationSchemaName = &(partitionCommand->name->schemaname);
				}
				else
				{
					continue;
				}

				/* prefix with schema name if it is not added already */
				SetSchemaNameIfNotExist(relationSchemaName, rightShardSchemaName);

				/*
				 * We will not append shard id to left shard name. This will be
				 * handled when we drop into RelayEventExtendNames.
				 */
				AppendShardIdToName(referencedTableName, rightShardId);
			}

			/* drop into RelayEventExtendNames for non-inter table commands */
			RelayEventExtendNames(parseTree, leftShardSchemaName, leftShardId);
			break;
		}

		default:
		{
			ereport(WARNING, (errmsg("unsafe statement type in name extension"),
							  errdetail("Statement type: %u", (uint32) nodeType)));
			break;
		}
	}
}


/*
 * UpdateWholeRowColumnReferencesWalker extends ColumnRef nodes that end with A_Star
 * with the given shardId.
 *
 * ColumnRefs that don't reference A_Star are not extended as catalog access isn't
 * allowed here and we don't otherwise have enough context to disambiguate a
 * field name that is identical to the table name.
 */
static bool
UpdateWholeRowColumnReferencesWalker(Node *node, uint64 *shardId)
{
	bool walkIsComplete = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, IndexElem))
	{
		IndexElem *indexElem = (IndexElem *) node;

		walkIsComplete = raw_expression_tree_walker(indexElem->expr,
													UpdateWholeRowColumnReferencesWalker,
													shardId);
	}
	else if (IsA(node, ColumnRef))
	{
		ColumnRef *columnRef = (ColumnRef *) node;
		Node *lastField = llast(columnRef->fields);

		if (IsA(lastField, A_Star))
		{
			/*
			 * ColumnRef fields list ends with an A_Star, so we can blindly
			 * extend the penultimate element with the shardId.
			 */
			int colrefFieldCount = list_length(columnRef->fields);
			Value *relnameValue = list_nth(columnRef->fields, colrefFieldCount - 2);
			Assert(IsA(relnameValue, String));

			AppendShardIdToName(&relnameValue->val.str, *shardId);
		}

		/* might be more than one ColumnRef to visit */
		walkIsComplete = false;
	}
	else
	{
		walkIsComplete = raw_expression_tree_walker(node,
													UpdateWholeRowColumnReferencesWalker,
													shardId);
	}

	return walkIsComplete;
}


/*
 * SetSchemaNameIfNotExist function checks whether schemaName is set and if it is not set
 * it sets its value to given newSchemaName.
 */
void
SetSchemaNameIfNotExist(char **schemaName, const char *newSchemaName)
{
	if ((*schemaName) == NULL)
	{
		*schemaName = pstrdup(newSchemaName);
	}
}


/*
 * AppendShardIdToName appends shardId to the given name. The function takes in
 * the name's address in order to reallocate memory for the name in the same
 * memory context the name was originally created in.
 */
void
AppendShardIdToName(char **name, uint64 shardId)
{
	char extendedName[NAMEDATALEN];
	int nameLength = strlen(*name);
	char shardIdAndSeparator[NAMEDATALEN];
	uint32 longNameHash = 0;
	int multiByteClipLength = 0;

	if (nameLength >= NAMEDATALEN)
	{
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("identifier must be less than %d characters",
							   NAMEDATALEN)));
	}

	SafeSnprintf(shardIdAndSeparator, NAMEDATALEN, "%c" UINT64_FORMAT,
				 SHARD_NAME_SEPARATOR, shardId);
	int shardIdAndSeparatorLength = strlen(shardIdAndSeparator);

	/*
	 * If *name strlen is < (NAMEDATALEN - shardIdAndSeparatorLength),
	 * it is safe merely to append the separator and shardId.
	 */

	if (nameLength < (NAMEDATALEN - shardIdAndSeparatorLength))
	{
		SafeSnprintf(extendedName, NAMEDATALEN, "%s%s", (*name), shardIdAndSeparator);
	}

	/*
	 * Otherwise, we need to truncate the name further to accommodate
	 * a sufficient hash value. The resulting name will avoid collision
	 * with other hashed names such that for any given schema with
	 * 90 distinct object names that are long enough to require hashing
	 * (typically 57-63 characters), the chance of a collision existing is:
	 *
	 * If randomly generated UTF8 names:
	 *     (1e-6) * (9.39323783788e-114) ~= (9.39e-120)
	 * If random case-insensitive ASCII names (letter first, 37 useful characters):
	 *     (1e-6) * (2.80380202421e-74) ~= (2.8e-80)
	 * If names sharing only N distinct 45- to 47-character prefixes:
	 *     (1e-6) * (1/N) = (1e-6/N)
	 *     1e-7 for 10 distinct prefixes
	 *     5e-8 for 20 distinct prefixes
	 *
	 * In practice, since shard IDs are globally unique, the risk of name collision
	 * exists only amongst objects that pertain to a single distributed table
	 * and are created for each shard: the table name and the names of any indexes
	 * or index-backed constraints. Since there are typically less than five such
	 * names, and almost never more than ten, the expected collision rate even in
	 * the worst case (ten names share same 45- to 47-character prefix) is roughly
	 * 1e-8: one in 100 million schemas will experience a name collision only if ALL
	 * 100 million schemas present the worst-case scenario.
	 */
	else
	{
		longNameHash = hash_any((unsigned char *) (*name), nameLength);
		multiByteClipLength = pg_mbcliplen(*name, nameLength, (NAMEDATALEN -
															   shardIdAndSeparatorLength -
															   10));
		SafeSnprintf(extendedName, NAMEDATALEN, "%.*s%c%.8x%s",
					 multiByteClipLength, (*name),
					 SHARD_NAME_SEPARATOR, longNameHash,
					 shardIdAndSeparator);
	}

	(*name) = (char *) repalloc((*name), NAMEDATALEN);
	int neededBytes = SafeSnprintf((*name), NAMEDATALEN, "%s", extendedName);
	if (neededBytes < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of memory: %m")));
	}
	else if (neededBytes >= NAMEDATALEN)
	{
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						errmsg("new name %s would be truncated at %d characters",
							   extendedName, NAMEDATALEN)));
	}
}


/*
 * shard_name() provides a PG function interface to AppendShardNameToId above.
 * Returns the name of a shard as a quoted schema-qualified identifier.
 */
Datum
shard_name(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	int64 shardId = PG_GETARG_INT64(1);

	char *qualifiedName = NULL;

	CheckCitusVersion(ERROR);

	if (shardId <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard_id cannot be zero or negative value")));
	}


	if (!OidIsValid(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("object_name does not reference a valid relation")));
	}

	char *relationName = get_rel_name(relationId);

	if (relationName == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("object_name does not reference a valid relation")));
	}

	AppendShardIdToName(&relationName, shardId);

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	if (strncmp(schemaName, "public", NAMEDATALEN) == 0)
	{
		qualifiedName = (char *) quote_identifier(relationName);
	}
	else
	{
		qualifiedName = quote_qualified_identifier(schemaName, relationName);
	}

	PG_RETURN_TEXT_P(cstring_to_text(qualifiedName));
}

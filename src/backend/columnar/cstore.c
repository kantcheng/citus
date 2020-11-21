/*-------------------------------------------------------------------------
 *
 * cstore.c
 *
 * This file contains...
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/rel.h"

#include "columnar/cstore.h"

/* Default values for option parameters */
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_NONE
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_BLOCK_ROW_COUNT 10000

int cstore_compression = DEFAULT_COMPRESSION_TYPE;
int cstore_stripe_row_count = DEFAULT_STRIPE_ROW_COUNT;
int cstore_block_row_count = DEFAULT_BLOCK_ROW_COUNT;

static const struct config_enum_entry cstore_compression_options[] =
{
	{ "none", COMPRESSION_NONE, false },
	{ "pglz", COMPRESSION_PG_LZ, false },
	{ "lz4", COMPRESSION_LZ4, false },
	{ NULL, 0, false }
};

void
cstore_init()
{
	DefineCustomEnumVariable("cstore.compression",
							 "Compression type for cstore.",
							 NULL,
							 &cstore_compression,
							 DEFAULT_COMPRESSION_TYPE,
							 cstore_compression_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("cstore.stripe_row_count",
							"Maximum number of tuples per stripe.",
							NULL,
							&cstore_stripe_row_count,
							DEFAULT_STRIPE_ROW_COUNT,
							STRIPE_ROW_COUNT_MINIMUM,
							STRIPE_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("cstore.block_row_count",
							"Maximum number of rows per block.",
							NULL,
							&cstore_block_row_count,
							DEFAULT_BLOCK_ROW_COUNT,
							BLOCK_ROW_COUNT_MINIMUM,
							BLOCK_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
}


/* ParseCompressionType converts a string to a compression type. */
CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	Assert(compressionTypeString != NULL);

	for (int type = 0; type < COMPRESSION_COUNT; type++)
	{
		if (strncmp(compressionTypeString, cstore_compression_options[type].name,
					NAMEDATALEN) == 0)
		{
			return type;
		}
	}

	return COMPRESSION_TYPE_INVALID;
}


/*
 * CompressionTypeStr returns string representation of a compression type.
 */
const char *
CompressionTypeStr(CompressionType type)
{
	if (type >= 0 && type < COMPRESSION_COUNT)
	{
		return cstore_compression_options[type].name;
	}
	else
	{
		return "invalid";
	}
}

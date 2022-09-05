/*-------------------------------------------------------------------------
 *
 * columnar.c
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

#include "citus_version.h"
#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_hooks.h"

/* Default values for option parameters */
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_CHUNK_ROW_COUNT 10000

#if HAVE_LIBZSTD
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_ZSTD
#elif HAVE_CITUS_LIBLZ4
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_LZ4
#else
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_PG_LZ
#endif

/* General GUC */
int columnar_compression = DEFAULT_COMPRESSION_TYPE;
int columnar_stripe_row_limit = DEFAULT_STRIPE_ROW_COUNT;
int columnar_chunk_group_row_limit = DEFAULT_CHUNK_ROW_COUNT;
int columnar_compression_level = 3;

/* Scan GUC */
bool columnar_enable_custom_scan = true;
bool columnar_enable_qual_pushdown = true;
double columnar_qual_pushdown_correlation_threshold  = 0.9;
int columnar_max_custom_scan_paths = 64;
int columnar_planner_debug_level = DEBUG3;

/* TableAM */
bool columnar_enable_version_checks  = true;

/* Vectorization GUC */
bool columnar_enable_vectorization = false;
bool columnar_use_simd = false;

static const struct config_enum_entry columnar_compression_options[] =
{
	{ "none", COMPRESSION_NONE, false },
	{ "pglz", COMPRESSION_PG_LZ, false },
#if HAVE_CITUS_LIBLZ4
	{ "lz4", COMPRESSION_LZ4, false },
#endif
#if HAVE_LIBZSTD
	{ "zstd", COMPRESSION_ZSTD, false },
#endif
	{ NULL, 0, false }
};

void
columnar_init(void)
{
	columnar_init_gucs();
	columnar_hooks_init();
	columnar_tableam_init();
}

void 
columnar_fini(void)
{
	columnar_hooks_fini();
	columnar_tableam_fini();
}

static const struct config_enum_entry debug_level_options[] = {
	{ "debug5", DEBUG5, false },
	{ "debug4", DEBUG4, false },
	{ "debug3", DEBUG3, false },
	{ "debug2", DEBUG2, false },
	{ "debug1", DEBUG1, false },
	{ "debug", DEBUG2, true },
	{ "info", INFO, false },
	{ "notice", NOTICE, false },
	{ "warning", WARNING, false },
	{ "log", LOG, false },
	{ NULL, 0, false }
};

void
columnar_init_gucs()
{
	DefineCustomEnumVariable("columnar.compression",
							 "Compression type for columnar.",
							 NULL,
							 &columnar_compression,
							 DEFAULT_COMPRESSION_TYPE,
							 columnar_compression_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("columnar.compression_level",
							"Compression level to be used with zstd.",
							NULL,
							&columnar_compression_level,
							3,
							COMPRESSION_LEVEL_MIN,
							COMPRESSION_LEVEL_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("columnar.stripe_row_limit",
							"Maximum number of tuples per stripe.",
							NULL,
							&columnar_stripe_row_limit,
							DEFAULT_STRIPE_ROW_COUNT,
							STRIPE_ROW_COUNT_MINIMUM,
							STRIPE_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("columnar.chunk_group_row_limit",
							"Maximum number of rows per chunk.",
							NULL,
							&columnar_chunk_group_row_limit,
							DEFAULT_CHUNK_ROW_COUNT,
							CHUNK_ROW_COUNT_MINIMUM,
							CHUNK_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	/* Scan */ 

	DefineCustomBoolVariable("columnar.enable_custom_scan",
							 gettext_noop("Enables the use of a custom scan to push projections and quals "
										  "into the storage layer."),
							 NULL,
							 &columnar_enable_custom_scan,
							 true,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("columnar.enable_qual_pushdown",
							 gettext_noop("Enables qual pushdown into columnar. This has no effect unless "
										  "columnar.enable_custom_scan is true."),
							 NULL,
							 &columnar_enable_qual_pushdown,
							 true,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL,
							 NULL, NULL, NULL);

	DefineCustomRealVariable("columnar.qual_pushdown_correlation_threshold",
							 gettext_noop("Correlation threshold to attempt to push a qual "
										  "referencing the given column. A value of 0 means "
										  "attempt to push down all quals, even if the column "
										  "is uncorrelated."),
							 NULL,
							 &columnar_qual_pushdown_correlation_threshold,
							 0.9,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("columnar.max_custom_scan_paths",
							gettext_noop("Maximum number of custom scan paths to generate "
										 "for a columnar table when planning."),
							NULL,
							&columnar_max_custom_scan_paths,
							64,
							1,
							1024,
							PGC_USERSET,
							GUC_NO_SHOW_ALL,
							NULL, NULL, NULL);

	DefineCustomEnumVariable("columnar.planner_debug_level",
							 "Message level for columnar planning information.",
							 NULL,
							 &columnar_planner_debug_level,
							 DEBUG3,
							 debug_level_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* TableAM */

	DefineCustomBoolVariable("columnar.enable_version_checks",
							 gettext_noop("Enables Version Check for Columnar"),
							 NULL,
							 &columnar_enable_version_checks,
							 true,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL,
							 NULL, NULL, NULL);
	
	/* Vectorization */
	
	DefineCustomBoolVariable("columnar.vectorization",
							 gettext_noop("Enables vectorization execution"),
							 NULL,
							 &columnar_enable_vectorization,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL,
							 NULL, 
							 NULL, 
							 NULL);

	DefineCustomBoolVariable("columnar.simd",
							 gettext_noop("Use SIMD instructions"),
							 NULL,
							 &columnar_use_simd,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL,
							 NULL, 
							 NULL, 
							 NULL);							 
}

/*
 * ParseCompressionType converts a string to a compression type.
 * For compression algorithms that are invalid or not compiled, it
 * returns COMPRESSION_TYPE_INVALID.
 */
CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	Assert(compressionTypeString != NULL);

	for (int compressionIndex = 0;
		 columnar_compression_options[compressionIndex].name != NULL;
		 compressionIndex++)
	{
		const char *compressionName = columnar_compression_options[compressionIndex].name;
		if (strncmp(compressionTypeString, compressionName, NAMEDATALEN) == 0)
		{
			return columnar_compression_options[compressionIndex].val;
		}
	}

	return COMPRESSION_TYPE_INVALID;
}


/*
 * CompressionTypeStr returns string representation of a compression type.
 * For compression algorithms that are invalid or not compiled, it
 * returns NULL.
 */
const char *
CompressionTypeStr(CompressionType requestedType)
{
	for (int compressionIndex = 0;
		 columnar_compression_options[compressionIndex].name != NULL;
		 compressionIndex++)
	{
		CompressionType compressionType =
			columnar_compression_options[compressionIndex].val;
		if (compressionType == requestedType)
		{
			return columnar_compression_options[compressionIndex].name;
		}
	}

	return NULL;
}

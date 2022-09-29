/*-------------------------------------------------------------------------
 *
 * utils.c
 *
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"

#include "columnar/vectorization/utils.h"

typedef struct VecTypeHashEntry
{
	Oid src;
	Oid dest;
} VecTypeHashEntry;

/* Map between the vectorized types and non-vectorized types */
static HTAB *hashMapN2V = NULL;
static HTAB *hashMapV2N = NULL;

#define TYPE_HASH_TABLE_SIZE 64

#define BUILTIN_TYPE_NUM 6

const char *typenames[] = {
	"any",
	"int2", 
	"int4", 
	"int8", 
	"bool",
	"text",
};

const char *vtypenames[] = {
	"vany",
	"vint2", 
	"vint4", 
	"vint8", 
	"vbool",
	"vtext"
};

/*
 * map non-vectorized type to vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid get_vector_type(Oid ntype)
{
	VecTypeHashEntry *entry = NULL;
	bool found = false;

	/* construct the hash table */
	if(NULL == hashMapN2V)
	{
		HASHCTL	hash_ctl;
		Oid		vtypid;
		Oid		typid;
		int		i;

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(VecTypeHashEntry);

		hashMapN2V = hash_create("vectorized_n2v", TYPE_HASH_TABLE_SIZE,
								 &hash_ctl, HASH_ELEM | HASH_BLOBS);
	

		Assert(sizeof(vtypenames) == sizeof(typenames));

		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			vtypid = TypenameGetTypid(vtypenames[i]);
			typid = TypenameGetTypid(typenames[i]);

			if (vtypid == InvalidOid)
				return InvalidOid;

			/* insert int4->vint4 mapping manually, may construct from catalog in future */
			entry = hash_search(hashMapN2V, &typid, HASH_ENTER, &found);
			entry->dest = vtypid;
		}
	}

	/* find the vectorized type in hash table */
	entry = hash_search(hashMapN2V, &ntype, HASH_FIND, &found);

	if(found)
		return entry->dest;

	return InvalidOid;
}


/*
 * Map vectorized type to non-vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid get_normal_type(Oid vtype)
{
	VecTypeHashEntry *entry = NULL;
	bool found = false;

	/* construct the hash table */
	if(NULL == hashMapV2N)
	{
		HASHCTL	hash_ctl;
		Oid		vtypid;
		Oid		typid;
		int		i;

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(VecTypeHashEntry);

		hashMapV2N = hash_create("vectorized_v2n", TYPE_HASH_TABLE_SIZE,
								 &hash_ctl, HASH_ELEM | HASH_BLOBS);

		Assert(sizeof(vtypenames) == sizeof(typenames));

		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			vtypid = TypenameGetTypid(vtypenames[i]);
			typid = TypenameGetTypid(typenames[i]);

			if (vtypid == InvalidOid)
				return InvalidOid;

			entry = hash_search(hashMapV2N, &vtypid, HASH_ENTER, &found);
			entry->dest = typid;
		}
	}

	/* find the vectorized type in hash table */
	entry = hash_search(hashMapV2N, &vtype, HASH_FIND, &found);

	if(found)
		return entry->dest;

	return InvalidOid;
}
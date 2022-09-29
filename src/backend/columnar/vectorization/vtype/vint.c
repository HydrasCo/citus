/*-------------------------------------------------------------------------
 *
 * vint.c
 *
 * $Id*
 * 
 *-------------------------------------------------------------------------
 */

#ifdef SIMD
#include <immintrin.h>
#include <popcntintrin.h>
#endif

#include "postgres.h"

#include "nodes/execnodes.h"
#include "executor/nodeAgg.h"

#include "columnar/columnar.h"
#include "columnar/vectorization/utils.h"
#include "columnar/vectorization/columnar_vectortupleslot.h"
#include "columnar/vectorization/vtype/vtype.h"


PG_FUNCTION_INFO_V1(vint8inc);
Datum vint8inc(PG_FUNCTION_ARGS)
{
	int64	arg = PG_GETARG_INT64(0);
	Vtype 	*batch = (Vtype *) PG_GETARG_POINTER(1);
	int64	result = arg;

#ifdef SIMD
	if (columnar_use_simd)
	{
		int pos = 0;

		int number_iterations = batch->dim / 32 + ( batch->dim % 32 ? 1 : 0);

		for (int n = 0; n < number_iterations; n++)
		{
			bool *b = (bool*) batch->skipref + pos;
			
			__m256i values = _mm256_loadu_si256((__m256i const *) b);

			bool *c = (bool*) batch->isDistinct + pos;

			__m256i distinctValues = _mm256_loadu_si256((__m256i const *) c);

			__m256i cntResult =  _mm256_and_si256(values, distinctValues);

			__m256i cmp_result = _mm256_cmpeq_epi8(cntResult, _mm256_setzero_si256());
			
			int binary_result = _mm256_movemask_epi8(cmp_result);
			
			result += _mm_popcnt_u32(binary_result);
			pos += 32;
		}
	}
	else
	{
#endif
		int i;

		for (i = 0; i < batch->dim; i++) 
		{
			if (batch->isnull[i])
				continue;

			if (!batch->skipref[i] && batch->isDistinct[i]) 
				result++;
		}
#ifdef SIMD
	}
#endif

	PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(vint8int8eq);
Datum vint8int8eq(PG_FUNCTION_ARGS)
{
	vint8 *arg1 = (vint8*)PG_GETARG_POINTER(0);
	int64 arg2 = PG_GETARG_INT64(1);

	vbool *res = build_vtype(BOOLOID, 1, VECTOR_BATCH_SIZE, arg1->skipref);

#ifdef SIMD
	if (columnar_use_simd)
	{
		__m256i cmp_values = _mm256_set1_epi64x(arg2);

		int number_iterations = arg1->dim / 4 + (arg1->dim % 4 ?  1 : 0);

		int pos = 0;

		for (int n = 0; n < number_iterations; n++)
		{
			int8 *b = (int8*) arg1->values + pos;

			bool *c = (bool*) res->values + pos;

			__m256i values = _mm256_loadu_si256((__m256i const *)b);

			__m256i cmp_result = _mm256_cmpgt_epi64(cmp_values, values);

			c[0] = _mm256_extract_epi8(cmp_result, 0);
			c[1] = _mm256_extract_epi8(cmp_result, 4);  
			c[2] = _mm256_extract_epi8(cmp_result, 8);  
			c[3] = _mm256_extract_epi8(cmp_result, 12);

			pos += 8;
		}
	}
	else
	{
#endif
		int size = 0; 
		int i = 0;
		size = arg1->dim;

		while(i < size) 
		{
			res->isnull[i] = arg1->isnull[i];

			if(!arg1->isnull[i]) 
			{
				int64 *a = (int64*) arg1->values + i;
				bool *c = (bool*) res->values + i;
				*c = (bool) (*a == arg2);
			}

			i++; 
		}
#ifdef SIMD
	}
#endif
	res->dim = arg1->dim; 
	PG_RETURN_POINTER(res);
	
}


PG_FUNCTION_INFO_V1(vint4int4lt);
Datum
vint4int4lt(PG_FUNCTION_ARGS)
{
	vint4 *arg1 = (vint4*)PG_GETARG_POINTER(0);
	int4 arg2 = PG_GETARG_INT32(1);

	vbool *res = build_vtype(BOOLOID, 1, VECTOR_BATCH_SIZE, arg1->skipref);

#ifdef SIMD
	if (columnar_use_simd)
	{
		__m256i cmp_values = _mm256_set1_epi32(arg2);

		int number_iterations = arg1->dim / 8 + (arg1->dim % 32 ?  1 : 0);

		int pos = 0;

		for (int n = 0; n < number_iterations; n++)
		{
			int4 *b = (int4*) arg1->values + pos;

			bool *c = (bool*) res->values + pos;

			__m256i values = _mm256_loadu_si256((__m256i const *)b);

			__m256i cmp_result = _mm256_cmpgt_epi32(cmp_values, values);

			c[0] = _mm256_extract_epi8(cmp_result, 0);
			c[1] = _mm256_extract_epi8(cmp_result, 4);  
			c[2] = _mm256_extract_epi8(cmp_result, 8);  
			c[3] = _mm256_extract_epi8(cmp_result, 12);
			c[4] = _mm256_extract_epi8(cmp_result, 16);
			c[5] = _mm256_extract_epi8(cmp_result, 20);  
			c[6] = _mm256_extract_epi8(cmp_result, 24);  
			c[7] = _mm256_extract_epi8(cmp_result, 28);

			pos += 8;
		}
	}
	else
	{
#endif
		int size = 0; 
		int i = 0;
		size = arg1->dim;

		while(i < size) 
		{
			res->isnull[i] = arg1->isnull[i];
			if(!arg1->isnull[i]) 
			{
				int4 *a = (int4*) arg1->values + i;
				bool *c = (bool*) res->values + i;
				*c = (bool) (*a < arg2);
			}
			i++; 
		}
#ifdef SIMD
	}
#endif

	res->dim = arg1->dim; 
	PG_RETURN_POINTER(res);
}


PG_FUNCTION_INFO_V1(vint4vint4pl);
Datum
vint4vint4pl(PG_FUNCTION_ARGS)
{
	vint4 *arg1 = (vint4*)PG_GETARG_POINTER(0);
	vint4 *arg2 = (vint4*)PG_GETARG_POINTER(1);

	vint4 *res = build_vtype(TypenameGetTypid("vint4"), 4, VECTOR_BATCH_SIZE, arg1->skipref);

#ifdef SIMD
	if (columnar_use_simd)
	{
        int number_iterations = arg1->dim / 8 + (arg1->dim % 8 ?  1 : 0);

        int pos = 0;

        for (int n = 0; n < number_iterations; n++)
        {
            int4 *left_vector = (int4*) arg1->values + pos;

            __m256i left_values = _mm256_loadu_si256((__m256i const *)left_vector);

            int4 *right_vector = (int4*) arg2->values + pos;

            __m256i right_values = _mm256_loadu_si256((__m256i const *)right_vector);

            __m256i and_result = _mm256_add_epi32(left_values, right_values);

            int4 *c = (int4*) res->values + pos;

            _mm256_storeu_si256((__m256i *)c, and_result);

            pos += 8;
        }
	}
	else
	{
#endif
		int i = 0;
		for(i = 0; i < arg1->dim; i++) 
		{
			res->isnull[i] = arg1->isnull[i] || arg1->isnull[i];

			if(!res->isnull[i]) 
			{
				int4 *a = (int4*) arg1->values + i;
				int4 *b = (int4*) arg2->values + i;
				int4 *c = (int4*) res->values + i;
				*c = (int4) (*a + *b);
			}
		}
#ifdef SIMD
	}
#endif

	res->dim = arg1->dim; 
	PG_RETURN_POINTER(res);
}
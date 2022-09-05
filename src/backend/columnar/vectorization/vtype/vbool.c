/*-------------------------------------------------------------------------
 *
 * vbool.c
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
#include "columnar/vectorization/columnar_vectortupleslot.h"
#include "columnar/vectorization/vtype/vtype.h"

PG_FUNCTION_INFO_V1(vbool_and_vbool);
Datum
vbool_and_vbool(PG_FUNCTION_ARGS)
{
	vbool *left = (vbool*)PG_GETARG_POINTER(0);
    
	vbool *right = (vbool*)PG_GETARG_POINTER(1);

    vbool *res = build_vtype(BOOLOID, 1, VECTOR_BATCH_SIZE, left->skipref);

#ifdef SIMD
    if (columnar_use_simd)
    {
        int number_iterations = left->dim / 32 + (left->dim % 32 ?  1 : 0);

        int pos = 0;

        for (int n = 0; n < number_iterations; n++)
        {
            bool *left_vector = (bool*) left->values + pos;

            __m256i left_values = _mm256_loadu_si256((__m256i const *)left_vector);

            bool *right_vector = (bool*) right->values + pos;

            __m256i right_values = _mm256_loadu_si256((__m256i const *)right_vector);

            __m256i and_result = _mm256_and_si256(left_values, right_values);

            bool *c = (bool*) res->values + pos;

            _mm256_storeu_si256((__m256i *)c, and_result);

            pos += 32;
        }
    }
    else
    {
#endif
        int number_iterations = left->dim;

        for (int n = 0; n < number_iterations; n++)
        {
            bool *a = (bool*) left->values + n;
            bool *b = (bool*) right->values + n;
            bool *c = (bool*) res->values + n;
            *c = *a & *b;
        }
#ifdef SIMD
    }
#endif

    PG_RETURN_POINTER(res);
}
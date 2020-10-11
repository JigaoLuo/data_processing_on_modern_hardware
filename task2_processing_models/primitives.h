#pragma once

#include "DBData.h"


static __inline__ size_t scanLong ( Tuple* inTuples, Tuple* outTuples, size_t n ) {
    size_t tid=0;
    for(; tid+8<n; tid+=8) {
        outTuples[tid] =   inTuples[tid];
        outTuples[tid+1] = inTuples[tid+1];
        outTuples[tid+2] = inTuples[tid+2];
        outTuples[tid+3] = inTuples[tid+3];
        outTuples[tid+4] = inTuples[tid+4];
        outTuples[tid+5] = inTuples[tid+5];
        outTuples[tid+6] = inTuples[tid+6];
        outTuples[tid+7] = inTuples[tid+7];
    }
    for ( ; tid<n; tid++) {
        outTuples[tid] = inTuples[tid];
    }
    return n;
}


static __inline__ size_t compareEquals ( Tuple* inTuples, long int val, Tuple* outTuples, size_t n ) {
    size_t nOut=0;
    size_t i=0;
    for(; i+8<n; i+=8) {
        if ( inTuples[i]   == val ) outTuples[nOut++] = inTuples[i];
        if ( inTuples[i+1] == val ) outTuples[nOut++] = inTuples[i+1];
        if ( inTuples[i+2] == val ) outTuples[nOut++] = inTuples[i+2];
        if ( inTuples[i+3] == val ) outTuples[nOut++] = inTuples[i+3];
        if ( inTuples[i+4] == val ) outTuples[nOut++] = inTuples[i+4];
        if ( inTuples[i+5] == val ) outTuples[nOut++] = inTuples[i+5];
        if ( inTuples[i+6] == val ) outTuples[nOut++] = inTuples[i+6];
        if ( inTuples[i+7] == val ) outTuples[nOut++] = inTuples[i+7];
    }
    for(; i<n; i++) {
        if ( inTuples[i] == val ) outTuples[nOut++] = inTuples[i];
    }
    return nOut;
}


static __inline__ size_t compareNotEquals ( Tuple* inTuples, long int val, Tuple* outTuples, size_t n ) {
    size_t nOut=0;
    size_t i=0;
    for(; i+8<n; i+=8) {
        if ( inTuples[i]   != val ) outTuples[nOut++] = inTuples[i];
        if ( inTuples[i+1] != val ) outTuples[nOut++] = inTuples[i+1];
        if ( inTuples[i+2] != val ) outTuples[nOut++] = inTuples[i+2];
        if ( inTuples[i+3] != val ) outTuples[nOut++] = inTuples[i+3];
        if ( inTuples[i+4] != val ) outTuples[nOut++] = inTuples[i+4];
        if ( inTuples[i+5] != val ) outTuples[nOut++] = inTuples[i+5];
        if ( inTuples[i+6] != val ) outTuples[nOut++] = inTuples[i+6];
        if ( inTuples[i+7] != val ) outTuples[nOut++] = inTuples[i+7];
    }  
    for(; i<n; i++) {
        if ( inTuples[i] != val ) outTuples[nOut++] = inTuples[i];
    }
    return nOut;
}


static __inline__ size_t compareSmaller ( Tuple* inTuples, long int val, Tuple* outTuples, size_t n ) {
    size_t nOut=0;
    size_t i=0;
    for(; i+8<n; i+=8) {
        if ( inTuples[i]   < val ) outTuples[nOut++] = inTuples[i];
        if ( inTuples[i+1] < val ) outTuples[nOut++] = inTuples[i+1];
        if ( inTuples[i+2] < val ) outTuples[nOut++] = inTuples[i+2];
        if ( inTuples[i+3] < val ) outTuples[nOut++] = inTuples[i+3];
        if ( inTuples[i+4] < val ) outTuples[nOut++] = inTuples[i+4];
        if ( inTuples[i+5] < val ) outTuples[nOut++] = inTuples[i+5];
        if ( inTuples[i+6] < val ) outTuples[nOut++] = inTuples[i+6];
        if ( inTuples[i+7] < val ) outTuples[nOut++] = inTuples[i+7];
    }  
    for(; i<n; i++) {
        if ( inTuples[i] < val ) outTuples[nOut++] = inTuples[i];
    }
    return nOut;
}


static __inline__ long int aggSum ( Tuple* inTuples, size_t n ) {
    long int sum = 0;
    size_t i=0;
    for(; i+8<n; i+=8) {
        sum += inTuples[i];
        sum += inTuples[i+1];
        sum += inTuples[i+2];
        sum += inTuples[i+3];
        sum += inTuples[i+4];
        sum += inTuples[i+5];
        sum += inTuples[i+6];
        sum += inTuples[i+7];
    }  
    for(; i<n; i++) {
        sum += inTuples[i];
    }
    return sum;
} 


static __inline__ size_t aggCount ( Tuple* inTuples, size_t n ) {
    return n;
}

 

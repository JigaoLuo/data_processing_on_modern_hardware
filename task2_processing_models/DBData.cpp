/**
 * @file
 *
 * Base operator methods and initialization of static variables.
 *
 * Based on code from Henning Funke (TU-Dortmund)
 *
 * @authors: Jana Giceva <jana.giceva@in.tum.de>, Alexander Beischl <beischl@in.tum.de>
 */
 
#include <iostream>
#include <stdlib.h>
#include "DBData.h"
#include "mappedmalloc.h"


Relation allocateRelation ( size_t capacity ) {
    Relation col;
    col.len = 0;
    col.capacity = capacity;
    col.r = (Tuple*) malloc ( sizeof ( Tuple ) * capacity );
    return col;
}


void freeRelation ( Relation col ) {
    free ( col.r );
}


void genData ( Relation* out, const char* filepath, size_t len ) {
    out->r = (Tuple*) malloc_memory_mapped_file ( sizeof(Tuple) * len, filepath );
    out->len = len;

    srand ( time ( nullptr ) );
    for(size_t i=0; i<len; i++) {
        out->r[i]=rand()%100;
    }
}


bool loadData ( Relation* out, const char* filepath, size_t len ) {
    if( access( filepath, F_OK ) == -1 ) {
        std::cout << "Cannot access file" << std::endl;
        return false;
    }
    size_t lenBytes;
    out->r = (Tuple*) map_memory_file ( filepath, &lenBytes );
    out->len = lenBytes / sizeof ( Tuple );
    if ( out->len != len ) {
        std::cout << "Size mismatch out->len: " << out->len << ", REL_LEN: " << len << std::endl;
        unmap_memory_file ( out->r );
        free_memory_mapped_file ( filepath );
        return false;
    }
    return true;
}



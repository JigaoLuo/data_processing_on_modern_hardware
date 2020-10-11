/**
 * @file
 *
 * Base operator methods and initialization of static variables.
 *
 * Based on code from Henning Funke (TU-Dortmund)
 *
 * @authors: Jana Giceva <jana.giceva@in.tum.de>, Alexander Beischl <beischl@in.tum.de>
 */
 
#pragma once

#include <cstddef>
#include <cassert>

typedef long int Tuple;
static_assert(sizeof(long int) == 8);

typedef struct Relation {
    Tuple* r;
    size_t len;
    size_t capacity;
    size_t start_offset = 0;
} Relation;


/**
  * @brief Allocate tuple array and initialize Relation attributes.
  */
Relation allocateRelation ( size_t capacity );


/**
  * @brief Free tuple array.
  */
void freeRelation ( Relation col );


/**
  * @brief Generate relation with uniform distribution in memory mapped file.
  * Fills the fields of out with corresponding sizes and pointers.
  * The generated file may be deleted to overwrite/free.
  */
void genData ( Relation* out, const char* filepath, size_t len );


/**
  * @brief Load relation from memory mapped fill into out.
  * Returns whether loading and size verification was successful.
  */
bool loadData ( Relation* out, const char* filepath, size_t len );



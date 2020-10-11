/**
 * @file
 *
 * Implementation of vector-at-a-time execution for relational operators.
 *
 */


#include <cassert>

#include "Operators.h"
#include "primitives.h"

void ScanOp::openVec() {
    this->cursor = 0;
    this->vector_finish = false;
}

Relation& ScanOp::nextVec() {
  if (this->oCol.start_offset != 0) {
    // Return the rest cached elements
    return oCol;
  } else if (this->vector_finish) {
    oCol.len = 0;
    return oCol;
  }
  oCol.len = scanLong ( table + cursor, oCol.r, (cursor + BATCH_SIZE <= this->tableSize) ?
                                                 BATCH_SIZE :
                                                (this->vector_finish = true, this->tableSize - cursor));
  cursor += oCol.len;
  return oCol;
}

void ScanOp::closeVec() {
  assert(cursor >= this->tableSize);
  assert(this->vector_finish);
}

void SelectionOp::openVec() {
  child->open();
}

Relation& SelectionOp::nextVec() {
  if (this->oCol.start_offset != 0) {
    // Return the rest cached elements
    return oCol;
  }
  Relation& in = child->nextVec();
  size_t out_counter = 0;
  while (in.len > 0) {
    size_t in_counter = in.start_offset;
    while (in_counter < in.len) {
      switch ( this->type ) {
        case PredicateType::EQUALS:
          if ( *(in.r + in_counter++) == this->compareConstant ) this->oCol.r[out_counter++] = in.r[in_counter - 1];
          break;
        case PredicateType::EQUALS_NOT:
          if ( *(in.r + in_counter++) != this->compareConstant ) this->oCol.r[out_counter++] = in.r[in_counter - 1];
          break;
        case PredicateType::SMALLER:
          if ( *(in.r + in_counter++) < this->compareConstant ) this->oCol.r[out_counter++] = in.r[in_counter - 1];
          break;
      }

      if (out_counter == BATCH_SIZE) {
        if (in_counter < in.len) in.start_offset = in_counter;
        oCol.len = out_counter;
        return oCol;
      }
    }
    in.start_offset = 0;    // Set to 0 := this child is completely processed
    in = child->nextVec();  // Get next new child
  }
  oCol.len = out_counter;
  return oCol;
}

void SelectionOp::closeVec() {
  child->close();
}

void AggregationOp::openVec() {
  child->open();
//  countTuple = 0;
//  this->hasMoreTuples = true;
  oCol.r[0] = 0;
}

Relation& AggregationOp::nextVec() {
  Relation& in = child->nextVec();
  oCol.len = 0;
  while (in.len > 0) {
    oCol.len = 1;
    size_t in_counter = oCol.start_offset;
    while (in_counter < in.len) {
      switch ( this->type ) {
        case AggregationOp::ReduceType::COUNT:
          oCol.r[0] += 1;
          break;
        case AggregationOp::ReduceType::SUM:
          oCol.r[0] += in.r[in_counter];
          break;
      }
      in_counter++;
    }
    in = child->nextVec();
  }
  return oCol;
}

void AggregationOp::closeVec() {
  child->close();
}
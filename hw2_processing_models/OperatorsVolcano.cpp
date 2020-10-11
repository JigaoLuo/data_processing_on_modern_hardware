/**
 * @file
 *
 * Base operator methods and initialization of static variables.
 *
 * Based on code from Henning Funke (TU-Dortmund)
 *
 * @authors: Jana Giceva <jana.giceva@in.tum.de>, Alexander Beischl <beischl@in.tum.de>
 */
 
#include "Operators.h"


void ScanOp::open() {
    this->cursor = 0;
}

Tuple* ScanOp::next() {
    while(true) {  
        if(cursor >= tableSize) {
            return nullptr;
        }
        return &table[cursor++]; 
    }
}

void ScanOp::close() {}

void SelectionOp::open() {
    child->open();
}

Tuple* SelectionOp::next() {
    while(true) {  
        Tuple* t = child->next();
        if(t == nullptr) return nullptr;
        switch ( this->type ) {
            case PredicateType::EQUALS:
                if ( *t == this->compareConstant ) return t;
                break;
            case PredicateType::EQUALS_NOT:
                if ( *t != this->compareConstant ) return t;
                break;
            case PredicateType::SMALLER:
                if ( *t < this->compareConstant ) return t;
                break;
        }
    }
    return nullptr;
}

void SelectionOp::close() {
    child->close();
}

void AggregationOp::open() {
    child->open();
    countTuple = 0;
    this->hasMoreTuples = true;
}

Tuple* AggregationOp::next() {
    if ( !this->hasMoreTuples ) {
        return nullptr;
    }
    while(true) {  
        Tuple* t = child->next();
        if(t == nullptr) break;
        if ( this->type == AggregationOp::ReduceType::COUNT ) {
            this->countTuple += 1;
        }
        if ( this->type == AggregationOp::ReduceType::SUM ) {
            this->countTuple += (*t);
        }
    }
    this->hasMoreTuples = false;
    return &this->countTuple;
}

void AggregationOp::close() {
    child->close();
}


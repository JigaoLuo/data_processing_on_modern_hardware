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
#include "primitives.h"


Relation ScanOp::getRelation() {
    oCol.len = scanLong ( table, oCol.r, this->tableSize );
    return oCol;
}

Relation SelectionOp::getRelation() {
    Relation in = child->getRelation();
    switch ( this->type ) {
        case PredicateType::EQUALS:
            in.len = compareEquals ( in.r, this->compareConstant, in.r, in.len );
            break;
        case PredicateType::EQUALS_NOT:
            in.len = compareNotEquals ( in.r, this->compareConstant, in.r, in.len );
            break;
        case PredicateType::SMALLER:
            in.len = compareSmaller ( in.r, this->compareConstant, in.r, in.len );
            break;
    }
    return in;
}

Relation AggregationOp::getRelation() {
    oCol.r[0] = 0;
    Relation in = child->getRelation();
    oCol.len = 1;
    if ( this->type == AggregationOp::ReduceType::COUNT ) {
        oCol.r[0] = aggCount ( in.r, in.len );
    }
    if ( this->type == AggregationOp::ReduceType::SUM ) {
        oCol.r[0] = aggSum ( in.r, in.len );
    }
    return oCol;
}

/**
 * @file
 *
 * Base operator methods and initialization of static variables.
 *
 * Based on code from Henning Funke (TU-Dortmund)
 *
 * @authors: Jana Giceva <jana.giceva@in.tum.de>, Alexander Beischl <beischl@in.tum.de>
 */
 
#include "BaseOperator.h"

RelOperator::RelOperator ( RelOperator *c ) {
    this->child = c;
    if ( c != nullptr ) {
        c->parent = this;
    }
}
    

void RelOperator::deletePlan() {
    if ( child != nullptr ) {
        child->deletePlan();
    }
    delete ( this );
}

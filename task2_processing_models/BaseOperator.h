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

#include <vector>
#include <iostream>

#include "DBData.h"

/**
 * @brief The operator base class
 * Super class for all operators. Serves as an interface for the supported processing models:
 * Volcano (Tuple-at-a-time), Columnar (Operator-at-a-time), Batch-Processing (Vector-at-a-time),
 * and Just-in-time compilation. 
 * The class provides a custom aligned allocator by overloading the new and delete operators.
 * This helps to avoid false sharing of cache lines during volcano execution.
 */
class RelOperator {
protected:    

    /* pointer to child operator */
    RelOperator* child = nullptr;

    /* pointer to parent operator */
    RelOperator* parent = nullptr;

    /* Size of concrete operator object; Needs to be set in constructor of derived classes. */
    size_t opSize = 0;
    

public:

    /* Set child parent/child pointer */
    RelOperator ( RelOperator *c );

    /* free memory */
    virtual ~RelOperator () {};

    /** 
     * @brief Overload new-operator for aligned allocation.
     * We use allocation of aligned memory to avoid false sharing.
     * Without aligned allocation false sharing may happen when operators,
     * that are executed on different cores share the same cache line.
     */
    static void* operator new ( size_t sz ) {

        /** The following commented line leads to usage of classical new
          * causing false sharing behaviour during parallel volcano execution.
          */
        //return ::operator new ( sz );        

        const size_t align = 64;

        void* ptr = aligned_alloc ( align, ( (sz / align) + 1) * align );
        if ( ptr == nullptr ) {
            std::cout << "allocation failed" << std::endl;
        }
        return ptr;

    }

    /** 
     * @brief We overload delete-operator for deallocation of aligned memory
     */
    static void operator delete ( void* ptr ) {
        
        /* The following line is classical delete with false sharing behaviour. */
        //::operator delete ( ptr );

        free ( ptr );
    }

    /** 
     * @brief Recursive deallocation and deletion of query plans.
     * Operators with varying numbers of children ( Join, Exchange Operator )
     * need to implement this method.
     */
    virtual void deletePlan ();
    
    /**
     * @brief Returns an estimate of the result size of the operator.
     * The estimate is based on the estimate of the child operators.
     * In the simplest case we pass relation sizes through the plan
     * for e.g. buffer allocation.
     */
    virtual size_t getSize ()   = 0;

    /**
     * Volcano style interface
     */
    virtual void open()   = 0;
    virtual Tuple* next() = 0;
    virtual void close()  = 0;

    /**
     * Operator-at-a-time interface
     */
    virtual Relation getRelation() = 0;

    /**
      * Vector-at-a-time interface
      */
    virtual void openVec() = 0;
    virtual Relation& nextVec() = 0;
    virtual void closeVec() = 0;
};




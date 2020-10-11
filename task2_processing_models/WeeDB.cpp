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
#include <iomanip>
#include <chrono>
#include <cassert>
#include <unistd.h>

#include "DBData.h"
#include "Operators.h"
#include "PerfEvent.hpp"

#ifndef RELATION_LEN
    #define RELATION_LEN 1024
//#define RELATION_LEN 200000000
#endif


/**
  * @brief Class to time executions.
  */
class Timer {
public:
    std::chrono::time_point<std::chrono::high_resolution_clock> start;

    /**
      * @brief Take timestamp at a position in the code
      */
    Timer() {
        start = std::chrono::high_resolution_clock::now();
    }

    /**
      * @brief Get milliseconds from timestamp to now
      */
    double get() {
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - start;
        return diff.count() * 1000;
    }
};


/**
  * @brief Output header line for csv
  */
void csvHeader () {
    std::cout << std::endl << "RELATION_LEN, tVolcano, tOperatorAtATime, tVectorAtATime" << std::endl;
}


/**
  * @brief Output timings of different execution models as csv line
  */
void csvStats ( double tVolc, double tOp, double tVec ) {
    std::cout << std::fixed;
    std::cout << std::setprecision(1);
    std::cout <<  RELATION_LEN << ", " << tVolc << ", " << tOp << ", " << tVec << std::endl;
}


/**
  * @brief Print query result
  */
void printRelation ( Relation r, bool onlyTupleCount = false ) {
    std::cout << "result " << r.len << " tuples" << std::endl; 
    if ( onlyTupleCount ) return;
    size_t i=0;
    for ( ; i<10 && i<r.len; i++ )
        std::cout << r.r[i] << std::endl;
    if ( i < (r.len-1) ) 
        std::cout << "[...]" << std::endl;
}


/**
  * @brief Execute query plan given by root with Volcano (Tuple-at-a-time)
  */
double execVolcano ( RelOperator* root ) {
    PerfEvent e;
    Timer tVolc = Timer();
    e.startCounters();
    Relation rel = allocateRelation ( root->getSize() );
    PullDriver::volcano ( root, &rel );
    e.stopCounters();
    std::cout << "Volcano (Tuple-at-a-time): ";
    printRelation ( rel );
    e.printReport(std::cout, RELATION_LEN); // use n as scale factor
    std::cout << std::endl;
    freeRelation ( rel );
    return tVolc.get();
}


/**
  * @brief Execute query plan given by root with Operator-at-a-time
  */
double execOperatorAtATime ( RelOperator* root ) {
    PerfEvent e;
    Timer tOp = Timer();
    e.startCounters();
    Relation resultRelation = root->getRelation();
    e.stopCounters();
    std::cout << "Materialization (Operator-at-a-time): ";
    printRelation ( resultRelation );
    e.printReport(std::cout, RELATION_LEN); // use n as scale factor
    std::cout << std::endl;
    return tOp.get();
}

/**
  * @brief Execute query plan given by root with Vectorization (Vector-at-a-time)
  */
double execVectorization ( RelOperator* root ) {
    PerfEvent e;
    Timer tVec = Timer();
    e.startCounters();
    Relation rel = allocateRelation ( root->getSize() );
    PullDriver::vectorization ( root, &rel );
    e.stopCounters();
    std::cout << "Vectorization (Vector-at-a-time): ";
    printRelation ( rel );
    e.printReport(std::cout, RELATION_LEN); // use n as scale factor
    std::cout << std::endl;
    freeRelation ( rel );
    return tVec.get();
}


/**
  * @brief Parse arguments, generate/read relation, build query plan, and execute.
    The different execution models are selected by arguments or by default we execute all.
  */
int main ( int argc, char* argv[] ) {

    // parse arguments
    bool doVol=false, doOp=false, doVec=false;
    std::string args;
    for (int i = 1; i < argc - 1; ++i) {
        args = args.append ( argv[i] );
    }

    if ( args.find ( "vol" ) != std::string::npos ) doVol = true;
    if ( args.find ( "op" ) != std::string::npos ) doOp = true;
    if ( args.find ( "vec" ) != std::string::npos ) doVec = true;
    if ( ! ( doVol || doOp || doVec) ) {
          doVol = true; doOp = true; doVec = true;
      }

    int query = argv[argc - 1][0] - '0';
    assert(query >= 0 && query <= 3);

    // load or generate relation data
    const char* dbFile = "db.dat";
    Relation relation;
    if ( !loadData ( &relation, dbFile, RELATION_LEN ) ) {
        std::cout << "Generating data.." << std::endl;
        genData ( &relation, dbFile, RELATION_LEN );
    }

    std::array<RelOperator*, 4> querys{};


    // build plan
    // Query0: SELECT SUM(x) FROM rel WHERE x <> 11 AND x <> 42 AND x <> 99　AND x <> 30 AND x <> 77;
    querys[0] = new AggregationOp ( AggregationOp::SUM,
        new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 77,
            new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 30,
                new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 99,
                    new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 42,
                        new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 11,
                            new ScanOp ( relation.r, RELATION_LEN )
                        )
                    )
                )
            )
        )
    );

    // Query1: SELECT x FROM rel WHERE x == 11;
    querys[1] = new SelectionOp ( SelectionOp::PredicateType::EQUALS, 11,
        new ScanOp ( relation.r, relation.len )
    );

    // Query2: SELECT SUM(x) FROM rel WHERE x <> 12 AND x <> 11 AND x <> 42 AND x <> 43;
    querys[2] = new AggregationOp ( AggregationOp::SUM,
        new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 43,
            new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 42,
                new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 11,
                    new SelectionOp ( SelectionOp::PredicateType::EQUALS_NOT, 12,
                        new ScanOp ( relation.r, relation.len )
                    )
                )
            )
        )
    );

    // Query3: SELECT sum(x) FROM rel;
    querys[3] = new AggregationOp ( AggregationOp::SUM,
        new ScanOp ( relation.r, RELATION_LEN )
    );

    double tVol=0.0, tOp=0.0, tVec=0.0;

    if ( doVec )  tVec  = execVectorization ( querys[query] );
    if ( doVol )  tVol  = execVolcano ( querys[query] );
    if ( doOp )   tOp   = execOperatorAtATime ( querys[query] );

    csvHeader ();
    csvStats ( tVol, tOp, tVec );

    for (auto q : querys) {
      q->deletePlan();
    }
}




















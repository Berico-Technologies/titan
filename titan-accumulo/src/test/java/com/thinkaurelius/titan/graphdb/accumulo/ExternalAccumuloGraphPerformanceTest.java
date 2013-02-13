package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalAccumuloGraphPerformanceTest extends TitanGraphPerformanceTest {

    public ExternalAccumuloGraphPerformanceTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration(), 0, 1, false);
    }


}

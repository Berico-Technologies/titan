package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalAccumuloGraphConcurrentTest extends TitanGraphConcurrentTest {

    public ExternalAccumuloGraphConcurrentTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }


}

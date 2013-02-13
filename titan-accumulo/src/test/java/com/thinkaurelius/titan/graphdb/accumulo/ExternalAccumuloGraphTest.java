package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalAccumuloGraphTest extends TitanGraphTest {

    public ExternalAccumuloGraphTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }

}

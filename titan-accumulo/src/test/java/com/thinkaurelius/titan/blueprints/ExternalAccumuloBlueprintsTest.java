package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ExternalAccumuloBlueprintsTest extends TitanBlueprintsTest {


    @Override
    public void startUp() {

    }

    @Override
    public void shutDown() {
  
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(AccumuloStorageSetup.getAccumuloGraphConfiguration());
        return graph;
    }

    @Override
    public void cleanUp() throws StorageException {
        AccumuloStoreManager s = new AccumuloStoreManager(
                AccumuloStorageSetup.getAccumuloGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }



}

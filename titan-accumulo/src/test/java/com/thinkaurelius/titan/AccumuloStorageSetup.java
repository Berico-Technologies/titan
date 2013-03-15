package com.thinkaurelius.titan;


import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.File;

public class AccumuloStorageSetup {


    public static Configuration getAccumuloStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "accumulo");
        config.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, "/tmp/graph");
        config.addProperty("username", "root");
        config.addProperty("password", "XXX");
        config.addProperty("instancename", "titan");
        config.addProperty("tablename", "titan");
        config.addProperty("hostname", "localhost");
        return config;
    }

    public static Configuration getAccumuloGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "accumulo");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, "/tmp/graph");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty("username", "root");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty("password", "XXX");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty("instancename", "titan");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty("tablename", "titan");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty("hostname", "localhost");
        return config;
    }

}

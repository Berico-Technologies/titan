package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class ExternalAccumuloMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new AccumuloStoreManager(getConfig());
    }

    private Configuration getConfig() {
        Configuration c = AccumuloStorageSetup.getAccumuloStorageConfiguration();
        return c;
    }
}

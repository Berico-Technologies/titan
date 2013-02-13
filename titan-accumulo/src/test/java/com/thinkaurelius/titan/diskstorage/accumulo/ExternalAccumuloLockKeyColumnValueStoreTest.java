package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;

public class ExternalAccumuloLockKeyColumnValueStoreTest
        extends LockKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        Configuration sc = AccumuloStorageSetup.getAccumuloStorageConfiguration();
        return new AccumuloStoreManager(sc);
    }
}

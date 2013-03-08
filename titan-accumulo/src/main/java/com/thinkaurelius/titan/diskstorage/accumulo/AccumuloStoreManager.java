package com.thinkaurelius.titan.diskstorage.accumulo;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Text;
//Conflicts with Titan's Mutation implementation - must be referenced explicitly
//import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.accumulo.AccumuloKeyColumnValueStore.toArray;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;

/**
 * First attempt at creating an Accumulo Store Manager
 * <p/>
 * This is not ready for production.
 *
 * @author Daniel West <dwest@bericotechnologies.com>
 */
public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(AccumuloStoreManager.class);

   
    public static final String INSTANCE_NAME_KEY = "instancename";
    public static final String INSTANCE_NAME_DEFAULT = "titan";
    private static final String TITAN_CONFIG_FILE_NAME = "titan-config.properties";
    public static final String TABLE_NAME_KEY = "tablename";
    public static final String TABLE_NAME_DEFAULT = "titan";
    private static final String USER_NAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";

    public static final int PORT_DEFAULT = 9997;

    public static final String ACCUMULO_CONFIGURATION_NAMESPACE = "accumulo-config";

    private final File directory;
    private final String tableName;
    private final String instanceName;
   

    private final Map<String, AccumuloKeyColumnValueStore> openStores;
    private final StoreFeatures features;
    private final Connector connector;

    public AccumuloStoreManager(org.apache.commons.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);
        this.tableName = config.getString(TABLE_NAME_KEY);
        this.instanceName = config.getString(INSTANCE_NAME_KEY);
        Preconditions.checkArgument(instanceName != null, "Need to specify instancename");
        
        String storageDir = config.getString(STORAGE_DIRECTORY_KEY);
        Preconditions.checkArgument(storageDir != null, "Need to specify storage directory");
        directory = new File(storageDir);
        
        String userName = config.getString(USER_NAME_KEY);
        Preconditions.checkArgument(userName != null, "Need to specify username");
        
        String password = config.getString(PASSWORD_KEY);
        Preconditions.checkArgument(password != null, "Need to specify password");
        
        String hostname = config.getString(GraphDatabaseConfiguration.HOSTNAME_KEY);
        
        ZooKeeperInstance zooKeeper = new ZooKeeperInstance(instanceName,hostname);
        try
        {
        	connector = zooKeeper.getConnector(userName, password.getBytes());
        }
        catch (AccumuloSecurityException e)
        {
        	log.error("Invalid Credentials to ZooKeeper: {}",e);
        	throw new PermanentStorageException(e);
        }
        catch (AccumuloException e)
        {
        	log.error("Unable to get connector from zookeeper: {}",e);
        	throw new TemporaryStorageException(e);
        }

        openStores = new HashMap<String, AccumuloKeyColumnValueStore>();

        features = new StoreFeatures();
        features.supportsScan = false;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;
       
    }


    @Override
    public String toString() {
        return "accumulo[" + tableName + "@" + super.toString() + "]";
    }

    @Override
    public void close() {
        openStores.clear();
    }


    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException{    	
        
        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(200000l, 200, 4);
        BatchWriter batchWriter;
		try {
			batchWriter = multiTableBatchWriter.getBatchWriter(tableName);
		} catch (AccumuloException e) {
			log.error("Accumulo Exception:{}",e);
			throw new PermanentStorageException(e);
		} catch (AccumuloSecurityException e) {
			log.error("Invalid Credentials:{}",e);
			throw new PermanentStorageException(e);
		} catch (TableNotFoundException e) {
			log.error("Table Does Not Exist:{}",e);
			throw new PermanentStorageException(e);
		}

        for (Map.Entry<String, Map<ByteBuffer, Mutation>> mutEntry : mutations.entrySet()) {
            Text columnFamily = new Text(mutEntry.getKey());
            for (Map.Entry<ByteBuffer, Mutation> rowMutation : mutEntry.getValue().entrySet()) {
                Text key = new Text(toArray(rowMutation.getKey().duplicate()));
                Mutation mutation = rowMutation.getValue();
                org.apache.accumulo.core.data.Mutation accumuloMutation = new org.apache.accumulo.core.data.Mutation(key);
                
                if (mutation.hasDeletions()) {
                    for (ByteBuffer b : mutation.getDeletions()) {
                        Text columnQualifier = new Text(toArray(b));
                        accumuloMutation.putDelete(columnFamily, columnQualifier, System.currentTimeMillis()-1);
                    }
                }

                if (mutation.hasAdditions()) {


                    for (Entry e : mutation.getAdditions()) {
                        Text column = new Text(toArray(e.getColumn()));
                        Value value = new Value(toArray(e.getValue()));
                        accumuloMutation.put(columnFamily, column, System.currentTimeMillis(),value);
                     
                    }
                }
                
                try {
					batchWriter.addMutation(accumuloMutation);
				} catch (MutationsRejectedException e) {
					log.error("Rejected Mutation: {}", e);
				}
            }
        }

        
        try {
			multiTableBatchWriter.close();
		} catch (MutationsRejectedException e) {
			log.error("Rejected Mutation: {}", e);
		}

    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws StorageException {
        if (openStores.containsKey(name))
            return openStores.get(name);

            if (!connector.tableOperations().exists(tableName)) {
            	try {
					connector.tableOperations().create(tableName);
				} catch (AccumuloException e) {
					log.error("Accumulo Exception:{}",e);
					throw new PermanentStorageException(e);
				} catch (AccumuloSecurityException e) {
					log.error("Invalid Credentials:{}",e);
					throw new PermanentStorageException(e);
				} catch (TableExistsException e) {
					log.error("Table Already Exists:{}",e);
					throw new TemporaryStorageException(e);
				}
            } 


        AccumuloKeyColumnValueStore store = new AccumuloKeyColumnValueStore(connector, tableName, name, this);
        openStores.put(name, store);

        return store;
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        return new AccumuloTransaction(level);
    }


    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     */
    @Override
    public void clearStorage() throws StorageException {
    	for (String table : connector.tableOperations().list())
    	{
    		if (table.equals("!METADATA"))
    		{
    			continue;
    		}
    		try {
				connector.tableOperations().delete(table);
			} catch (AccumuloException e) {
				log.error("Unknown Error: {}",e);
				throw new TemporaryStorageException(e);
			} catch (AccumuloSecurityException e) {
				log.debug("Invalid credentials: {}",e);
				throw new PermanentStorageException(e);
			} catch (TableNotFoundException e) {
				log.debug("Attempting to delete already removed table - ignoring: {}",e);
			}
    		
    	}
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        File configFile = getConfigFile(directory);

        if (!configFile.exists()) //property has not been defined
            return null;

        Preconditions.checkArgument(configFile.isFile());
        try {
            Configuration config = new PropertiesConfiguration(configFile);
            return config.getString(key, null);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Could not read from configuration file", e);
        }
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        File configFile = getConfigFile(directory);

        try {
            PropertiesConfiguration config = new PropertiesConfiguration(configFile);
            config.setProperty(key, value);
            config.save();
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Could not save configuration file", e);
        }
    }

    private static File getConfigFile(File dbDirectory) {
        return new File(dbDirectory.getAbsolutePath() + File.separator + TITAN_CONFIG_FILE_NAME);
    }
    
    static byte[] toArray(ByteBuffer b) {
        if (0 == b.arrayOffset() && b.limit() == b.array().length)
            return b.array();

        byte[] result = new byte[b.limit()];
        b.duplicate().get(result);
        return result;
    }

}

package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Attempt at adding Accumulo data storage
 * <p/>
 * This is inherently flawed at the moment and should not be used in production.
 * <p/>
 */
public class AccumuloKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(AccumuloKeyColumnValueStore.class);

    private final String tableName;
    private final Connector connector;
    public static final Serializer serial = new KryoSerializer(true);

    // This is cf.getBytes()
    private final String columnFamily;
    private final Text columnFamilyText;
    private final AccumuloStoreManager storeManager;

    AccumuloKeyColumnValueStore(Connector connector, String tableName,
                             String columnFamily, AccumuloStoreManager storeManager) {
        this.tableName = tableName;
        this.connector = connector;
        this.columnFamily = columnFamily;
        this.columnFamilyText = new Text(columnFamily);
        this.storeManager = storeManager;
    }

    @Override
    public void close() throws StorageException {
    	
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column,
                          StoreTransaction txh) throws StorageException {
        byte[] colBytes = toArray(column);

        Scanner scanner;
   		try {
   			scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
   		} catch (TableNotFoundException e) {
   			log.error("Table non-existant when scanning: {}",e);
   			throw new PermanentStorageException(e);
   		}



        Key rangeKey = new Key(new Text(toArray(key)),columnFamilyText, new Text(colBytes));

        Range range = new Range(rangeKey,true,null,false);



           
//        IteratorSetting cfg = new IteratorSetting(1, RegExFilter.class);
//           try {
//   			RegExFilter.setRegexs(cfg, null, columnFamily, Text.decode(colBytes), null, false);
//   		} catch (CharacterCodingException e) {
//   			log.warn("Invalid Character Encoding: {}",e);
//   		}
           scanner.setRange(range);


           //scanner.addScanIterator(cfg);
           for (java.util.Map.Entry<Key,Value> entry : scanner) {
               if (entry.getValue().getSize() > 0)
			    return ByteBuffer.wrap(entry.getValue().get());
           }

        return null;
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
                                     StoreTransaction txh) throws StorageException {
        return (get(key,column,txh) != null);
    }


    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {

        Scanner scanner;
		try {
			scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
		} catch (TableNotFoundException e) {
			log.error("Table non-existant when scanning: {}",e);
			throw new PermanentStorageException(e);
		}


        Range range = new Range(new Text(toArray(key)));

        scanner.setRange(range);
        scanner.fetchColumnFamily(columnFamilyText);

        Iterator<java.util.Map.Entry<Key, Value>> iter = scanner.iterator();
        
        return iter.hasNext();
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, int limit, StoreTransaction txh) throws StorageException {

        byte[] colStartBytes = columnEnd.hasRemaining() ? toArray(columnStart) : null;
        byte[] colEndBytes = columnEnd.hasRemaining() ? toArray(columnEnd) : null;

        return getHelper(key, colStartBytes,colEndBytes,limit);
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
                                ByteBuffer columnEnd, StoreTransaction txh) throws StorageException {

        byte[] colStartBytes = columnEnd.hasRemaining() ? toArray(columnStart) : null;
        byte[] colEndBytes = columnEnd.hasRemaining() ? toArray(columnEnd) : null;


        return getHelper(key, colStartBytes,colEndBytes);
    }

    private List<Entry> getHelper(ByteBuffer key,
            byte[] startColumn, byte[] endColumn) throws StorageException {
    	return getHelper(key,startColumn,endColumn,-1);
    }
    

    private List<Entry> getHelper(ByteBuffer key,
                                  byte[] startColumn, byte[] endColumn, int limit) throws StorageException {
    	        
        Scanner scanner;
		try {
			scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
		} catch (TableNotFoundException e) {
			log.error("Table non-existant when scanning: {}",e);
			throw new PermanentStorageException(e);
		}
        Key startKey = null, endKey = null;
        if (startColumn != null)
        {
            startKey = new Key(new Text(toArray(key)),new Text(columnFamily), new Text(startColumn));
        }
        if (endColumn != null)
        {
            endKey = new Key(new Text(toArray(key)),new Text(columnFamily), new Text(endColumn));
        }


        Range range = new Range(startKey,true,endKey,false);

        scanner.setRange(range);
        
        List<Entry> list = new LinkedList<Entry>();
        
        Iterator<java.util.Map.Entry<Key, Value>> iter = scanner.iterator();

        int counter = 0;

        while (iter.hasNext()) {
	        	java.util.Map.Entry<Key,Value> e = iter.next();
                    Text colq = e.getKey().getColumnQualifier();

                    list.add(new Entry(ByteBuffer.wrap(colq.getBytes()),ByteBuffer.wrap(e.getValue().get())));
                    counter++;
                    if (counter == limit)
                    {
                        break;
                    }
        }
        

        return list;
    }

    static byte[] toArray(ByteBuffer b) {
        if (0 == b.arrayOffset() && b.limit() == b.array().length)
            return b.array();

        byte[] result = new byte[b.limit()];
        b.duplicate().get(result);
        return result;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions,
                       List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {

        byte[] keyBytes = toArray(key);
        BatchWriter batchWriter;
		try {
			batchWriter = connector.createBatchWriter(tableName, 50000l, 300, 4);
		} catch (TableNotFoundException e) {
			log.error("Table not found: {}", e);
			throw new PermanentStorageException(e);
		}
        org.apache.accumulo.core.data.Mutation accumuloMutation = new org.apache.accumulo.core.data.Mutation(new Text(keyBytes));
        


        // Deletes
        if (null != deletions && 0 != deletions.size()) {
            for (ByteBuffer del : deletions) {
                accumuloMutation.putDelete(columnFamilyText, new Text(toArray(del.duplicate())));
                log.debug("Removing {} {}", columnFamilyText, new Text(toArray(del.duplicate())));
            }
        }

        // Inserts
        if (null != additions && 0 != additions.size()) {
            for (Entry e : additions) {
                Text columnQualifier = new Text(toArray(e.getColumn().duplicate()));
                Value value = new Value(toArray(e.getValue().duplicate()));
                accumuloMutation.put(columnFamilyText, columnQualifier, value);
                log.debug("Adding {} {}", columnQualifier.toString(), value.toString());
            }
        }

        try{
            batchWriter.addMutation(accumuloMutation);
        } catch (MutationsRejectedException e) {
            log.debug("Rows Rejected: {}",e);
        }

        try {
			batchWriter.close();
		} catch (MutationsRejectedException e) {
			log.debug("Rows Rejected: {}",e);
		}
        
    }

    public void mutateMany(
            Map<ByteBuffer, Mutation> mutations,
            StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column,
                            ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return columnFamily;
    }

}

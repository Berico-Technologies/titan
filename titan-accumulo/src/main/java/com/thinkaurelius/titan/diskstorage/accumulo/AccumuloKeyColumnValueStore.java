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
           
           Range range = new Range(new Text(toArray(key)));

           
           IteratorSetting cfg = new IteratorSetting(1, RegExFilter.class);
           try {
   			RegExFilter.setRegexs(cfg, null, columnFamily, Text.decode(colBytes), null, false);
   		} catch (CharacterCodingException e) {
   			log.warn("Invalid Character Encoding: {}",e);
   		}
           scanner.setRange(range);
           scanner.addScanIterator(cfg);
               
           Iterator<java.util.Map.Entry<Key, Value>> iter = scanner.iterator();
           
           if (iter.hasNext())
           {
        	   java.util.Map.Entry<Key,Value> e = iter.next();
        	   
        	   try {
				return Text.encode(e.getValue().toString());
			} catch (CharacterCodingException e1) {
	   			log.warn("Invalid Character Encoding: {}",e1);
	   			return null;
	   		}
           }
           else
           {
        	   return null;
           }
           
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
                                     StoreTransaction txh) throws StorageException {
        Scanner scanner;
		try {
			scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
		} catch (TableNotFoundException e) {
			log.error("Table non-existant when scanning: {}",e);
			throw new PermanentStorageException(e);
		}
        
        Range range = new Range(new Text(toArray(key)));

        IteratorSetting cfg = new IteratorSetting(1, RegExFilter.class);
        String columnName = "";
		try {
			columnName = Text.decode(toArray(column));
		} catch (CharacterCodingException e) {
			log.error("Invalid encoding: {}",e);
		}
		RegExFilter.setRegexs(cfg, null, columnFamily, columnName, null, false);
		log.debug("Column Family: {} Column Value: {}", columnFamily, columnName);
        scanner.setRange(range);
        scanner.addScanIterator(cfg);
            
        Iterator<java.util.Map.Entry<Key, Value>> iter = scanner.iterator();
        log.debug("The key column exists: {}",iter.hasNext());
        return iter.hasNext();
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

        
        IteratorSetting cfg = new IteratorSetting(1, RegExFilter.class);
        RegExFilter.setRegexs(cfg, null, columnFamily, null, null, false);
        scanner.setRange(range);
        scanner.addScanIterator(cfg);
            
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
    
    
    /*
     * This is inherently flawed.
     * It's doing a manual scan on the entire row.
     * There has to be a way to have Accumulo at least begin on a specific column
     */
    private List<Entry> getHelper(ByteBuffer key,
                                  byte[] startColumn, byte[] endColumn, int limit) throws StorageException {
    	        
        Scanner scanner;
		try {
			scanner = connector.createScanner(tableName, Constants.NO_AUTHS);
		} catch (TableNotFoundException e) {
			log.error("Table non-existant when scanning: {}",e);
			throw new PermanentStorageException(e);
		}
        
        Range range = new Range(new Text(toArray(key)));
        scanner.setRange(range);
        
        List<Entry> list = new LinkedList<Entry>();
        
        Iterator<java.util.Map.Entry<Key, Value>> iter = scanner.iterator();
        boolean scan = (startColumn == null);
        int counter = 0;
        

        while (iter.hasNext()) {
            try
            {
	        	java.util.Map.Entry<Key,Value> e = iter.next();
	        	
	        	Text colq = e.getKey().getColumnQualifier();
	        	if (toArray(Text.encode(colq.toString())).equals(startColumn))
	        	{
	        		scan = true;
	        	}
	        	
	        	if (toArray(Text.encode(colq.toString())).equals(endColumn))
	        	{
	        		scan = false;
	        		break;
	        	}
	        	
	        	if (scan)
	        	{
	        		list.add(new Entry(Text.encode(colq.toString()),Text.encode(e.getValue().toString())));
	        		counter++;
	        		if (counter == limit)
	        		{
	        			break;
	        		}
	        	}
            }
            catch (CharacterCodingException e)
            {
            	log.debug("Invalid character encoding : {}",e);
            }
        }
        

        return list;
    }

    /*
     * This method exists because HBase's API generally deals in
     * whole byte[] arrays.  That is, data are always assumed to
     * begin at index zero and run through the native length of
     * the array.  These assumptions are reflected, for example,
     * in the classes hbase.client.Get and hbase.client.Scan.
     * These assumptions about arrays are not generally true when
     * dealing with ByteBuffers.
     * <p>
     * This method first checks whether the array backing the
     * ByteBuffer argument indeed satisfies the assumptions described
     * above.  If so, then this method returns the backing array.
     * In other words, this case returns {@code b.array()}.
     * <p>
     * If the ByteBuffer argument does not satisfy the array
     * assumptions described above, then a new native array of length
     * {@code b.limit()} is created.  The ByteBuffer's contents
     * are copied into the new native array without modifying the
     * state of {@code b} (using {@code b.duplicate()}).  The new
     * native array is then returned.
     *
     */
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
                log.debug("Removing {} {}", columnFamilyText, new Text(toArray(del.duplicate()).toString()));
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

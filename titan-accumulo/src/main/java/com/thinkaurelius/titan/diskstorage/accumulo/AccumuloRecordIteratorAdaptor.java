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
import org.apache.accumulo.core.client.*;
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
 * @author Daniel West <dwest@bericotechnologies.com>
 *         Date: 3/15/13
 *         Time: 10:42 AM
 * Built to be an adaptor from a RowIterator to the Titan RecordIterator specifically for Key Scans.
 */
public class AccumuloRecordIteratorAdaptor implements RecordIterator<ByteBuffer> {

    protected RowIterator iterator;

    public AccumuloRecordIteratorAdaptor(RowIterator iterator)
    {
        this.iterator = iterator;
    }

    public boolean hasNext() throws StorageException
    {
        return iterator.hasNext();
    }

    public ByteBuffer next() throws StorageException
    {
        return ByteBuffer.wrap(iterator.next().next().getKey().getRow().getBytes());
    }

    public void close() throws StorageException
    {

    }


}

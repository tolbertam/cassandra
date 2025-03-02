/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;

public class NativeCellTest extends CQLTester
{

    private static final Logger logger = LoggerFactory.getLogger(NativeCellTest.class);
    private static final NativeAllocator nativeAllocator = new NativePool(Integer.MAX_VALUE,
                                                                          Integer.MAX_VALUE,
                                                                          1f,
                                                                          () -> ImmediateFuture.success(true)).newAllocator(null);
    private static final OpOrder.Group group = new OpOrder().start();
    private static Random rand;

    @BeforeClass
    public static void setUp()
    {
        long seed = System.currentTimeMillis();
        logger.info("Seed : {}", seed);
        rand = new Random(seed);
    }

    @Test
    public void testCells()
    {
        for (int run = 0 ; run < 1000 ; run++)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(rndclustering());
            int count = 1 + rand.nextInt(10);
            for (int i = 0 ; i < count ; i++)
                rndcd(builder);
            test(builder.build());
        }
    }

    private static Clustering<?> rndclustering()
    {
        int count = 1 + rand.nextInt(100);
        ByteBuffer[] values = new ByteBuffer[count];
        int size = rand.nextInt(65535);
        for (int i = 0 ; i < count ; i++)
        {
            int twiceShare = 1 + (2 * size) / (count - i);
            int nextSize = Math.min(size, rand.nextInt(twiceShare));
            if (nextSize < 10 && rand.nextBoolean())
                continue;

            byte[] bytes = new byte[nextSize];
            rand.nextBytes(bytes);
            values[i] = ByteBuffer.wrap(bytes);
            size -= nextSize;
        }
        return Clustering.make(values);
    }

    private static void rndcd(Row.Builder builder)
    {
        ColumnMetadata col = rndcol();
        if (!col.isComplex())
        {
            builder.addCell(rndcell(col));
        }
        else
        {
            int count = 1 + rand.nextInt(100);
            for (int i = 0 ; i < count ; i++)
                builder.addCell(rndcell(col));
        }
    }

    private static ColumnMetadata rndcol()
    {
        UUID uuid = new UUID(rand.nextLong(), rand.nextLong());
        boolean isComplex = rand.nextBoolean();
        return new ColumnMetadata("",
                                  "",
                                  ColumnIdentifier.getInterned(uuid.toString(), false),
                                    isComplex ? new SetType<>(BytesType.instance, true) : BytesType.instance,
                                  -1,
                                  ColumnMetadata.Kind.REGULAR,
                                  null);
    }

    private static Cell<?> rndcell(ColumnMetadata col)
    {
        long timestamp = rand.nextLong();
        int ttl = rand.nextInt();
        long localDeletionTime = ThreadLocalRandom.current().nextLong(Cell.getVersionedMaxDeletiontionTime() + 1);
        byte[] value = new byte[rand.nextInt(sanesize(expdecay()))];
        rand.nextBytes(value);
        CellPath path = null;
        if (col.isComplex())
        {
            byte[] pathbytes = new byte[rand.nextInt(sanesize(expdecay()))];
            rand.nextBytes(value);
            path = CellPath.create(ByteBuffer.wrap(pathbytes));
        }

        return new BufferCell(col, timestamp, ttl, localDeletionTime, ByteBuffer.wrap(value), path);
    }

    private static int expdecay()
    {
        return 1 << Integer.numberOfTrailingZeros(Integer.lowestOneBit(rand.nextInt()));
    }

    private static int sanesize(int randomsize)
    {
        return Math.min(Math.max(1, randomsize), 1 << 26);
    }

    private static void test(Row row)
    {
        Row nrow = row.clone(nativeAllocator.cloner(group));
        Row brow = row.clone(HeapCloner.instance);
        Assert.assertEquals(row, nrow);
        Assert.assertEquals(row, brow);
        Assert.assertEquals(nrow, brow);

        Assert.assertEquals(row.clustering(), nrow.clustering());
        Assert.assertEquals(row.clustering(), brow.clustering());
        Assert.assertEquals(nrow.clustering(), brow.clustering());

        Assert.assertEquals(row.clustering().dataSize(), nrow.clustering().dataSize());
        Assert.assertEquals(row.clustering().dataSize(), brow.clustering().dataSize());

        ClusteringComparator comparator = new ClusteringComparator(UTF8Type.instance);
        Assert.assertEquals(0, comparator.compare(row.clustering(), nrow.clustering()));
        Assert.assertEquals(0, comparator.compare(row.clustering(), brow.clustering()));
        Assert.assertEquals(0, comparator.compare(nrow.clustering(), brow.clustering()));

        assertCellsDataSize(row, nrow);
        assertCellsDataSize(row, brow);

    }

    private static void assertCellsDataSize(Row row1, Row row2)
    {
        Iterator<Cell<?>> row1Iterator = row1.cells().iterator();
        Iterator<Cell<?>> row2Iterator = row2.cells().iterator();
        while (row1Iterator.hasNext())
        {
            Cell cell1 = row1Iterator.next();
            Cell cell2 = row2Iterator.next();
            Assert.assertEquals(cell1.dataSize(), cell2.dataSize());
        }
    }

}

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

package org.apache.cassandra.repair.autorepair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.autorepair.IAutoRepairTokenRangeSplitter.RepairAssignment;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.autorepair.RepairRangeSplitter.TABLE_BATCH_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairRangeSplitterTest extends CQLTester
{
    private RepairRangeSplitter repairRangeSplitter;
    private String tableName;
    private static Range<Token> FULL_RANGE;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        FULL_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(), DatabaseDescriptor.getPartitioner().getMaximumToken());
    }

    @Before
    public void setUp() {
        repairRangeSplitter = new RepairRangeSplitter(Collections.emptyMap());
        tableName = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT)");
    }

    @Test
    public void testSizePartitionCount() throws Throwable
    {
        insertAndFlushTable(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Refs<SSTableReader> sstables = RepairRangeSplitter.getSSTableReaderRefs(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableName, FULL_RANGE);
        assertEquals(10, sstables.iterator().next().getEstimatedPartitionSize().count());
        RepairRangeSplitter.SizeEstimate sizes = RepairRangeSplitter.getSizesForRangeOfSSTables(KEYSPACE, tableName, FULL_RANGE, sstables);
        assertEquals(10, sizes.partitions);
    }

    @Test
    public void testSizePartitionCountSplit() throws Throwable
    {
        int[] values = new int[10000];
        for (int i = 0; i < values.length; i++)
            values[i] = i + 1;
        insertAndFlushTable(tableName, values);
        Iterator<Range<Token>> range = AutoRepairUtils.split(FULL_RANGE, 2).iterator();
        Range<Token> tokenRange1 = range.next();
        Range<Token> tokenRange2 = range.next();
        Assert.assertFalse(range.hasNext());

        Refs<SSTableReader> sstables1 = RepairRangeSplitter.getSSTableReaderRefs(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableName, tokenRange1);
        Refs<SSTableReader> sstables2 = RepairRangeSplitter.getSSTableReaderRefs(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableName, tokenRange2);
        RepairRangeSplitter.SizeEstimate sizes1 = RepairRangeSplitter.getSizesForRangeOfSSTables(KEYSPACE, tableName, tokenRange1, sstables1);
        RepairRangeSplitter.SizeEstimate sizes2 = RepairRangeSplitter.getSizesForRangeOfSSTables(KEYSPACE, tableName, tokenRange2, sstables2);
        // +-5% because HLL merge and the applying of range size approx ratio causes estimation errors
        assertTrue(Math.abs(10000 - (sizes1.partitions + sizes2.partitions)) <= 60);
    }

    @Test
    public void testReorderByPriorityWithDifferentPriorities() {
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '3'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '1'}");

        // Test reordering assignments with different priorities
        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table1));
        RepairAssignment assignment2 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table2));
        RepairAssignment assignment3 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table3));

        // Assume these priorities based on the repair type
        List<RepairAssignment> assignments = new ArrayList<>(Arrays.asList(assignment1, assignment2, assignment3));

        repairRangeSplitter.reorderByPriority(assignments, AutoRepairConfig.RepairType.FULL);

        // Verify the order is by descending priority
        assertEquals(assignment2, assignments.get(0));
        assertEquals(assignment1, assignments.get(1));
        assertEquals(assignment3, assignments.get(2));
    }

    @Test
    public void testReorderByPriorityWithSamePriority() {
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");

        // Test reordering assignments with the same priority
        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table1));
        RepairAssignment assignment2 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table2));
        RepairAssignment assignment3 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table3));

        List<RepairAssignment> assignments = new ArrayList<>(Arrays.asList(assignment1, assignment2, assignment3));

        repairRangeSplitter.reorderByPriority(assignments, AutoRepairConfig.RepairType.FULL);

        // Verify the original order is preserved as all priorities are the same
        assertEquals(assignment1, assignments.get(0));
        assertEquals(assignment2, assignments.get(1));
        assertEquals(assignment3, assignments.get(2));
    }

    @Test
    public void testReorderByPriorityWithMixedPriorities() {
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table2 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '3'}");
        String table3 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '2'}");
        String table4 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '1'}");

        // Test reordering assignments with mixed priorities
        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table1));
        RepairAssignment assignment2 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table2));
        RepairAssignment assignment3 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table3));
        RepairAssignment assignment4 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table4));

        List<RepairAssignment> assignments = new ArrayList<>(Arrays.asList(assignment1, assignment2, assignment3, assignment4));

        repairRangeSplitter.reorderByPriority(assignments, AutoRepairConfig.RepairType.FULL);

        // Verify the order: highest priority first, then preserved order for same priority
        assertEquals(assignment2, assignments.get(0)); // Priority 3
        assertEquals(assignment1, assignments.get(1)); // Priority 2
        assertEquals(assignment3, assignments.get(2)); // Priority 2
        assertEquals(assignment4, assignments.get(3)); // Priority 1
    }

    @Test
    public void testReorderByPriorityWithEmptyList() {
        // Test with an empty list (should remain empty)
        List<RepairAssignment> assignments = new ArrayList<>();
        repairRangeSplitter.reorderByPriority(assignments, AutoRepairConfig.RepairType.FULL);
        assertTrue(assignments.isEmpty());
    }

    @Test
    public void testReorderByPriorityWithOneElement() {
        // Test with a single element (should remain unchanged)
        String table1 = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) WITH repair_full = {'enabled': 'true', 'priority': '5'}");

        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, KEYSPACE, Collections.singletonList(table1));

        List<RepairAssignment> assignments = new ArrayList<>(Collections.singletonList(assignment1));

        repairRangeSplitter.reorderByPriority(assignments, AutoRepairConfig.RepairType.FULL);

        assertEquals(assignment1, assignments.get(0)); // Single element should remain in place
    }

    @Test
    public void testGetRepairAssignmentsForTable_NoSSTables() {
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(Murmur3Partitioner.instance.getMinimumToken(), Murmur3Partitioner.instance.getMaximumToken()));
        List<RepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForTable(AutoRepairConfig.RepairType.FULL, CQLTester.KEYSPACE, tableName, ranges);
        assertEquals(0, assignments.size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_Single() throws Throwable {
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(), DatabaseDescriptor.getPartitioner().getMaximumToken()));
        insertAndFlushSingleTable(tableName);
        List<RepairAssignment> assignments = repairRangeSplitter.getRepairAssignmentsForTable(AutoRepairConfig.RepairType.FULL, CQLTester.KEYSPACE, tableName, ranges);
        assertEquals(1, assignments.size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_BatchingTables() throws Throwable {
        repairRangeSplitter = new RepairRangeSplitter(Collections.singletonMap(TABLE_BATCH_LIMIT, "2"));
        Collection<Range<Token>> ranges = Collections.singleton(FULL_RANGE);

        List<String> tableNames = createAndInsertTables(3);
        List<RepairAssignment> assignments = repairRangeSplitter.getRepairAssignments(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableNames, ranges);

        // We expect two assignments, one with table1 and table2 batched, and one with table3
        assertEquals(2, assignments.size());
        assertEquals(2, assignments.get(0).getTableNames().size());
        assertEquals(1, assignments.get(1).getTableNames().size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_BatchSize() throws Throwable {
        repairRangeSplitter = new RepairRangeSplitter(Collections.singletonMap(TABLE_BATCH_LIMIT, "2"));
        Collection<Range<Token>> ranges = Collections.singleton(FULL_RANGE);

        List<String> tableNames = createAndInsertTables(2);
        List<RepairAssignment> assignments = repairRangeSplitter.getRepairAssignments(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableNames, ranges);

        // We expect one assignment, with two tables batched
        assertEquals(1, assignments.size());
        assertEquals(2, assignments.get(0).getTableNames().size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_NoBatching() throws Throwable {
        repairRangeSplitter = new RepairRangeSplitter(Collections.singletonMap(TABLE_BATCH_LIMIT, "1"));
        Collection<Range<Token>> ranges = Collections.singleton(FULL_RANGE);

        List<String> tableNames = createAndInsertTables(3);
        List<RepairAssignment> assignments = repairRangeSplitter.getRepairAssignments(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableNames, ranges);

        assertEquals(3, assignments.size());
    }

    @Test
    public void testGetRepairAssignmentsForTable_AllBatched() throws Throwable {
        repairRangeSplitter = new RepairRangeSplitter(Collections.singletonMap(TABLE_BATCH_LIMIT, "100"));
        Collection<Range<Token>> ranges = Collections.singleton(FULL_RANGE);

        List<String> tableNames = createAndInsertTables(5);
        List<RepairAssignment> assignments = repairRangeSplitter.getRepairAssignments(AutoRepairConfig.RepairType.FULL, KEYSPACE, tableNames, ranges);

        assertEquals(1, assignments.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeEmptyAssignments() {
        // Test when the list of assignments is empty
        List<RepairAssignment> emptyAssignments = Collections.emptyList();
        RepairRangeSplitter.merge(emptyAssignments);
    }

    @Test
    public void testMergeSingleAssignment() {
        // Test when there is only one assignment in the list
        String keyspaceName = "testKeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        RepairAssignment assignment = new RepairAssignment(FULL_RANGE, keyspaceName, tableNames);
        List<RepairAssignment> assignments = Collections.singletonList(assignment);

        RepairAssignment result = RepairRangeSplitter.merge(assignments);

        assertEquals(FULL_RANGE, result.getTokenRange());
        assertEquals(keyspaceName, result.getKeyspaceName());
        assertEquals(new HashSet<>(tableNames), new HashSet<>(result.getTableNames()));
    }

    @Test
    public void testMergeMultipleAssignmentsWithSameTokenRangeAndKeyspace() {
        // Test merging multiple assignments with the same token range and keyspace
        String keyspaceName = "testKeyspace";
        List<String> tableNames1 = Arrays.asList("table1", "table2");
        List<String> tableNames2 = Arrays.asList("table2", "table3");

        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, keyspaceName, tableNames1);
        RepairAssignment assignment2 = new RepairAssignment(FULL_RANGE, keyspaceName, tableNames2);
        List<RepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairAssignment result = RepairRangeSplitter.merge(assignments);

        assertEquals(FULL_RANGE, result.getTokenRange());
        assertEquals(keyspaceName, result.getKeyspaceName());
        assertEquals(new HashSet<>(Arrays.asList("table1", "table2", "table3")), new HashSet<>(result.getTableNames()));
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeDifferentTokenRange() {
        // Test merging assignments with different token ranges
        Iterator<Range<Token>> range = AutoRepairUtils.split(FULL_RANGE, 2).iterator(); // Split the full range into two ranges ie (0-100, 100-200
        Range<Token> tokenRange1 = range.next();
        Range<Token> tokenRange2 = range.next();
        Assert.assertFalse(range.hasNext());

        String keyspaceName = "testKeyspace";
        List<String> tableNames = Arrays.asList("table1", "table2");

        RepairAssignment assignment1 = new RepairAssignment(tokenRange1, keyspaceName, tableNames);
        RepairAssignment assignment2 = new RepairAssignment(tokenRange2, keyspaceName, tableNames);
        List<RepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairRangeSplitter.merge(assignments); // Should throw IllegalStateException
    }

    @Test(expected = IllegalStateException.class)
    public void testMergeDifferentKeyspaceName() {
        // Test merging assignments with different keyspace names
        List<String> tableNames = Arrays.asList("table1", "table2");

        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, "keyspace1", tableNames);
        RepairAssignment assignment2 = new RepairAssignment(FULL_RANGE, "keyspace2", tableNames);
        List<RepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairRangeSplitter.merge(assignments); // Should throw IllegalStateException
    }

    @Test
    public void testMergeWithDuplicateTables() {
        // Test merging assignments with duplicate table names
        String keyspaceName = "testKeyspace";
        List<String> tableNames1 = Arrays.asList("table1", "table2");
        List<String> tableNames2 = Arrays.asList("table2", "table3");

        RepairAssignment assignment1 = new RepairAssignment(FULL_RANGE, keyspaceName, tableNames1);
        RepairAssignment assignment2 = new RepairAssignment(FULL_RANGE, keyspaceName, tableNames2);
        List<RepairAssignment> assignments = Arrays.asList(assignment1, assignment2);

        RepairAssignment result = RepairRangeSplitter.merge(assignments);

        // The merged result should contain all unique table names
        assertEquals(new HashSet<>(Arrays.asList("table1", "table2", "table3")), new HashSet<>(result.getTableNames()));
    }


    private void insertAndFlushSingleTable(String tableName) throws Throwable {
        execute("INSERT INTO %s (k, v) values (?, ?)", 1, 1);
        flush();
    }

    private List<String> createAndInsertTables(int count) throws Throwable {
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String tableName = createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT)");
            tableNames.add(tableName);
            insertAndFlushTable(tableName);
        }
        return tableNames;
    }

    private void insertAndFlushTable(String tableName) throws Throwable {
        insertAndFlushTable(tableName, 1);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, tableName);
    }
    private void insertAndFlushTable(String tableName, int... vals) throws Throwable {
        for (int i : vals)
        {
            executeFormattedQuery("INSERT INTO " + KEYSPACE + '.' + tableName + " (k, v) values (?, ?)", i, i);
        }
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, tableName);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }
}

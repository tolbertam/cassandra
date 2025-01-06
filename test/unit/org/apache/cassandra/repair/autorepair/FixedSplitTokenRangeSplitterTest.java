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
import java.util.Set;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.cql3.CQLTester.Fuzzed.setupSeed;
import static org.apache.cassandra.cql3.CQLTester.Fuzzed.updateConfigs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FixedSplitTokenRangeSplitterTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE1 = "tbl1";
    private static final String TABLE2 = "tbl2";
    private static final String TABLE3 = "tbl3";

    private static final Map<String, String> splitterParams = Collections.singletonMap(FixedSplitTokenRangeSplitter.NUMBER_OF_SUBRANGES, Integer.toString(4));

    @Parameterized.Parameter()
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<AutoRepairConfig.RepairType> repairTypes()
    {
        return Arrays.asList(AutoRepairConfig.RepairType.values());
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        setupSeed();
        updateConfigs();
        DatabaseDescriptor.setPartitioner("org.apache.cassandra.dht.Murmur3Partitioner");
        ServerTestUtils.prepareServerNoRegister();

        Token t1 = new Murmur3Partitioner.LongToken(-9223372036854775808L);
        Token t2 = new Murmur3Partitioner.LongToken(-3074457345618258603L);
        Token t3 = new Murmur3Partitioner.LongToken(3074457345618258602L);
        Set<Token> tokens = new HashSet<>();
        tokens.add(t1);
        tokens.add(t2);
        tokens.add(t3);

        ServerTestUtils.registerLocal(tokens);
        // Ensure that the on-disk format statics are loaded before the test run
        Version.LATEST.onDiskFormat();
        StorageService.instance.doAutoRepairSetup();

        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
    }

    @Test
    public void testTokenRangesSplitByTable()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(repairType, false);
        int totalTokenRanges = 3;
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(totalTokenRanges, tokens.size());
        int numberOfSplits = 4;
        List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++)
        {
            for (Range<Token> range : tokens)
            {
                expectedToken.addAll(AutoRepairUtils.split(range, numberOfSplits));
            }
        }

        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.buildSingleKeyspacePlan(repairType, KEYSPACE, TABLE1, TABLE2, TABLE3);

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = new FixedSplitTokenRangeSplitter(repairType, splitterParams)
                                                                  .getRepairAssignments(true, plan);

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertEquals(totalTokenRanges*numberOfSplits*tables.size(), assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        int expectedTableIndex = -1;
        for (int i = 0; i < totalTokenRanges * numberOfSplits * tables.size(); i++)
        {
            if (i % (totalTokenRanges * numberOfSplits) == 0)
            {
                expectedTableIndex++;
            }
        }

        expectedTableIndex = -1;
        // should be a set of ranges for each table.
        for (int i = 0; i<totalTokenRanges*numberOfSplits*tables.size(); i++)
        {
            if (i % (totalTokenRanges*numberOfSplits) == 0)
            {
                expectedTableIndex++;
            }
            assertEquals(expectedToken.get(i), assignments.get(i).getTokenRange());
            assertEquals(Arrays.asList(tables.get(expectedTableIndex)), assignments.get(i).getTableNames());
        }
    }

    @Test
    public void testTokenRangesSplitByKeyspace()
    {
        AutoRepairService.instance.getAutoRepairConfig().setRepairByKeyspace(repairType, true);
        int totalTokenRanges = 3;
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        assertEquals(totalTokenRanges, tokens.size());
        int numberOfSplits = 4;
        List<String> tables = Arrays.asList(TABLE1, TABLE2, TABLE3);
        List<Range<Token>> expectedToken = new ArrayList<>();
        for (Range<Token> range : tokens)
        {
            expectedToken.addAll(AutoRepairUtils.split(range, numberOfSplits));
        }

        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.buildSingleKeyspacePlan(repairType, KEYSPACE, TABLE1, TABLE2, TABLE3);

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = new FixedSplitTokenRangeSplitter(repairType, splitterParams)
                                                                  .getRepairAssignments(true, plan);

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        assertEquals(totalTokenRanges*numberOfSplits, assignments.size());
        assertEquals(expectedToken.size(), assignments.size());

        // should only be one set of ranges for the entire keyspace.
        for (int i = 0; i<totalTokenRanges*numberOfSplits; i++)
        {
            assertEquals(expectedToken.get(i), assignments.get(i).getTokenRange());
            assertEquals(tables, assignments.get(i).getTableNames());
        }
    }

    @Test
    public void testTokenRangesNoSplitByDefault()
    {
        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(KEYSPACE);
        int totalTokenRanges = 3;
        assertEquals(totalTokenRanges, tokens.size());
        List<Range<Token>> expectedToken = new ArrayList<>(tokens);

        List<PrioritizedRepairPlan> plan = PrioritizedRepairPlan.buildSingleKeyspacePlan(repairType, KEYSPACE, TABLE1);

        Iterator<KeyspaceRepairAssignments> keyspaceAssignments = new FixedSplitTokenRangeSplitter(repairType, Collections.emptyMap()).getRepairAssignments(true, plan);

        // should be only 1 entry for the keyspace.
        assertTrue(keyspaceAssignments.hasNext());
        KeyspaceRepairAssignments keyspace = keyspaceAssignments.next();
        assertFalse(keyspaceAssignments.hasNext());

        List<RepairAssignment> assignments = keyspace.getRepairAssignments();
        assertNotNull(assignments);

        // should be 3 entries for the table which covers each token range.
        assertEquals(totalTokenRanges, assignments.size());
        for (int i = 0; i < totalTokenRanges; i++)
        {
            assertEquals(expectedToken.get(i), assignments.get(i).getTokenRange());
        }
    }
}

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

package org.apache.cassandra.dht.tokenallocator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleLocationProvider;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TokenAllocationTest
{
    static IPartitioner oldPartitioner;
    static Random rand = new Random(1);

    @BeforeClass
    public static void beforeClass() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void before() throws ConfigurationException
    {
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.syncInstanceForTest());
        ClusterMetadataService.instance().log().unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
    }

    @Before
    public void after() throws ConfigurationException
    {
        ClusterMetadataService.unsetInstance();
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
    }

    private static TokenAllocation createForTest(ClusterMetadata metadata, int replicas, int numTokens)
    {
        return TokenAllocation.create(metadata.locator.local().datacenter, metadata, replicas, numTokens);
    }

    @Test
    public void testAllocateTokensForKeyspace() throws UnknownHostException
    {
        int vn = 16;
        String ks = "TokenAllocationTestKeyspace3";
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KeyspaceMetadata.create(ks, KeyspaceParams.simple(5)));
        ClusterMetadata metadata = generateFakeEndpoints(10, vn);
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        allocateTokensForKeyspace(vn, ks, metadata, addr);
    }

    @Test
    public void testAllocateTokensForLocalRF() throws UnknownHostException
    {
        int vn = 16;
        int allocateTokensForLocalRf = 3;
        ClusterMetadata metadata = generateFakeEndpoints(10, vn);
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        allocateTokensForLocalReplicationFactor(vn, allocateTokensForLocalRf, metadata, addr);
    }

    private Collection<Token> allocateTokensForKeyspace(int vnodes, String keyspace, ClusterMetadata metadata, InetAddressAndPort addr)
    {
        AbstractReplicationStrategy rs = metadata.schema.getKeyspaces().get(keyspace).get().replicationStrategy;
        TokenAllocation tokenAllocation = TokenAllocation.create(metadata, rs, vnodes);
        return allocateAndVerify(vnodes, addr, tokenAllocation);
    }

    private void allocateTokensForLocalReplicationFactor(int vnodes, int rf, ClusterMetadata metadata, InetAddressAndPort addr)
    {
        TokenAllocation tokenAllocation = createForTest(metadata, rf, vnodes);
        allocateAndVerify(vnodes, addr, tokenAllocation);
    }

    private Collection<Token> allocateAndVerify(int vnodes, InetAddressAndPort addr, TokenAllocation tokenAllocation)
    {
        SummaryStatistics os = tokenAllocation.getAllocationRingOwnership(addr);
        Collection<Token> tokens = tokenAllocation.allocate(addr);
        assertEquals(vnodes, tokens.size());
        SummaryStatistics ns = tokenAllocation.getAllocationRingOwnership(addr);
        if (ns.getStandardDeviation() > os.getStandardDeviation())
        {
            fail(String.format("Token allocation unexpectedly increased standard deviation.\nStats before:\n%s\nStats after:\n%s", os, ns));
        }
        return tokens;
    }

    @Test
    public void testAllocateTokensNetworkStrategyOneRack() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(1, 3);
    }

    @Test(expected = ConfigurationException.class)
    public void testAllocateTokensNetworkStrategyTwoRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(2, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyThreeRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(3, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyFiveRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(5, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyOneRackOneReplica() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(1, 1);
    }


    public void testAllocateTokensNetworkStrategy(int rackCount, int replicas) throws UnknownHostException
    {
        int vn = 16;
        String ks = "TokenAllocationTestNTSKeyspace" + rackCount + replicas;
        String dc = "1";
        String otherDc = "15";
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(ks, KeyspaceParams.nts(dc, replicas, otherDc, "15"));

        // register these 2 nodes so that the DCs exist, otherwise the CREATE KEYSPACE will be rejected
        // but don't join them, we don't assign any tokens to these nodes
        ClusterMetadataTestHelper.register(InetAddressAndPort.getByName("127.1.0.99"), dc, Integer.toString(0));
        ClusterMetadataTestHelper.register(InetAddressAndPort.getByName("127.15.0.99"), otherDc, Integer.toString(0));
        ClusterMetadataTestHelper.addOrUpdateKeyspace(keyspace);

        for (int i = 0; i < rackCount; ++i)
            generateFakeEndpoints(10, vn, dc, Integer.toString(i));
        InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + ".0.99");
        allocateTokensForKeyspace(vn, ks, ClusterMetadata.current(), addr);
        // Note: Not matching replication factor in second datacentre, but this should not affect us.
    }

    @Test
    public void testAllocateTokensRfEqRacks() throws UnknownHostException
    {
        int vn = 8;
        int replicas = 3;
        int rackCount = replicas;
        String ks = "TokenAllocationTestNTSKeyspaceRfEqRack";
        String dc = "1";
        String otherDc = "15";
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(ks, KeyspaceParams.nts(dc, replicas, otherDc, "15"));

        // register these 2 nodes so that the DCs exist, otherwise the CREATE KEYSPACE will be rejected
        // but don't join them, we don't assign any tokens to these nodes
        ClusterMetadataTestHelper.register(InetAddressAndPort.getByName("127.1.0.255"), dc, Integer.toString(0));
        ClusterMetadataTestHelper.register(InetAddressAndPort.getByName("127.15.0.255"), otherDc, Integer.toString(0));
        ClusterMetadataTestHelper.addOrUpdateKeyspace(keyspace);

        int base = 5;
        for (int i = 0; i < rackCount; ++i)
            generateFakeEndpoints(base << i, vn, dc, Integer.toString(i));     // unbalanced racks

        int cnt = 5;
        for (int i = 0; i < cnt; ++i)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127." + dc + ".0." + (99 + i));
            ClusterMetadataTestHelper.register(endpoint, dc, Integer.toString(0));
            Collection<Token> tokens = allocateTokensForKeyspace(vn, ks, ClusterMetadata.current(), endpoint);
            ClusterMetadataTestHelper.join(endpoint, new HashSet<>(tokens));
        }

        double target = 1.0 / (base + cnt);
        double permittedOver = 1.0 / (2 * vn + 1) + 0.01;

        Map<InetAddress, Float> ownership = StorageService.instance.effectiveOwnership(ks);
        boolean failed = false;
        for (Map.Entry<InetAddress, Float> o : ownership.entrySet())
        {
            int rack = o.getKey().getAddress()[2];
            if (rack != 0)
                continue;

            System.out.format("Node %s owns %f ratio to optimal %.2f\n", o.getKey(), o.getValue(), o.getValue() / target);
            if (o.getValue() / target > 1 + permittedOver)
                failed = true;
        }
        Assert.assertFalse(String.format("One of the nodes in the rack has over %.2f%% overutilization.", permittedOver * 100), failed);
    }

    /**
     * TODO: This scenario isn't supported very well. Investigate a multi-keyspace version of the algorithm.
     */
    @Test
    public void testAllocateTokensMultipleKeyspaces() throws UnknownHostException
    {
        final int TOKENS = 16;

        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forClientTools());
        generateFakeEndpoints(10, TOKENS);
        // pre-register the nodes which we are going to allocate tokens to this is necessary as
        // Location info is obtained by the allocator from cluster metadata, not the snitch
        Map<InetAddressAndPort, NodeId> endpointToIdMap = new HashMap<>(10);
        for (int i=11; i<=20; ++i)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + (i + 1));
            NodeId id = ClusterMetadataTestHelper.register(endpoint, SimpleLocationProvider.LOCATION);
            endpointToIdMap.put(endpoint, id);
        }
        ClusterMetadata metadata = ClusterMetadata.current();

        TokenAllocation rf2Allocator = createForTest(metadata, 2, TOKENS);
        TokenAllocation rf3Allocator = createForTest(metadata, 3, TOKENS);

        SummaryStatistics rf2StatsBefore = rf2Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());
        SummaryStatistics rf3StatsBefore = rf3Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());

        TokenAllocation current = rf3Allocator;
        TokenAllocation next = rf2Allocator;

        for (Map.Entry<InetAddressAndPort, NodeId> entry : endpointToIdMap.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            NodeId id = entry.getValue();
            Set<Token> tokens = new HashSet<>(current.allocate(endpoint));
            // Update tokens on next to verify ownership calculation below
            next.updateTokensForNode(id, tokens);
            TokenAllocation tmp = current;
            current = next;
            next = tmp;
        }

        SummaryStatistics rf2StatsAfter = rf2Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());
        SummaryStatistics rf3StatsAfter = rf3Allocator.getAllocationRingOwnership(FBUtilities.getBroadcastAddressAndPort());

        verifyImprovement(rf2StatsBefore, rf2StatsAfter);
        verifyImprovement(rf3StatsBefore, rf3StatsAfter);
    }

    private void verifyImprovement(SummaryStatistics os, SummaryStatistics ns)
    {
        if (ns.getStandardDeviation() > os.getStandardDeviation())
        {
            fail(String.format("Token allocation unexpectedly increased standard deviation.\nStats before:\n%s\nStats after:\n%s", os, ns));
        }
    }

    private ClusterMetadata generateFakeEndpoints(int nodes, int vnodes) throws UnknownHostException
    {
        // This is used by tests which previously relied on the ServerTestUtils snitch impl which has
        // 127.0.0.4 in datacenter2 and every other node in datacenter1
        IPartitioner p = ClusterMetadata.current().tokenMap.partitioner();
        for (int i = 1; i <= nodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0." + (i + 1));
            String dc = (i + 1 == 4) ? ServerTestUtils.DATA_CENTER_REMOTE : ServerTestUtils.DATA_CENTER;
            ClusterMetadataTestHelper.register(addr, dc, ServerTestUtils.RACK1);
            Set<Token> tokens = new HashSet<>(vnodes);
            for (int j = 0; j < vnodes; ++j)
                tokens.add(p.getRandomToken(rand));
            ClusterMetadataTestHelper.join(addr, tokens);
        }
        // Register (but don't join/allocate tokens for) 127.0.0.1)
        ClusterMetadataTestHelper.register(FBUtilities.getBroadcastAddressAndPort(),
                                           ServerTestUtils.DATA_CENTER,
                                           ServerTestUtils.RACK1);
        return ClusterMetadata.current();
    }

    private ClusterMetadata generateFakeEndpoints(int nodes, int vnodes, String dc, String rack) throws UnknownHostException
    {
        System.out.printf("Adding %d nodes to dc=%s, rack=%s.%n", nodes, dc, rack);
        IPartitioner p = ClusterMetadata.current().tokenMap.partitioner();

        for (int i = 1; i <= nodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + '.' + rack + '.' + (i + 1));
            ClusterMetadataTestHelper.register(addr, dc, rack);
            Set<Token> tokens = new HashSet<>(vnodes);
            for (int j = 0; j < vnodes; ++j)
                tokens.add(p.getRandomToken(rand));
            ClusterMetadataTestHelper.join(addr, tokens);
        }
        return ClusterMetadata.current();
    }
}

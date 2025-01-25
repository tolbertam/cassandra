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

package org.apache.cassandra.tools.nodetool;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class AutoRepairStatusTest
{
    @Mock
    private static NodeProbe probe;

    private ByteArrayOutputStream cmdOutput;

    private static AutoRepairStatus cmd;

    @Parameterized.Parameter()
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<AutoRepairConfig.RepairType> repairTypes()
    {
        return Arrays.asList(AutoRepairConfig.RepairType.values());
    }

    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        cmdOutput = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(cmdOutput);
        when(probe.output()).thenReturn(new Output(out, out));
        cmd = new AutoRepairStatus();
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.loadConfig();
        setAutoRepairEnabled(true);
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(AutoRepairConfig.RepairType.FULL, true);
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairEnabled(AutoRepairConfig.RepairType.INCREMENTAL, true);
//        when(probe.getAutoRepairConfig()).thenReturn(DatabaseDescriptor.getAutoRepairConfig());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecuteWithoutRepairType()
    {
        cmd.repairType = null;
        cmd.execute(probe);
    }

    @Test
    public void testExecuteWithNoNodes()
    {
        cmd.repairType = repairType.name();

        cmd.execute(probe);
        assertEquals("Active Repairs\n" +
        "NONE          \n", cmdOutput.toString());
    }

    @Test
    public void testExecute()
    {
        when(probe.getOnGoingRepairHostIds(repairType.name())).thenReturn(ImmutableSet.of("host1", "host2", "host3", "host4"));
        cmd.repairType = repairType.name();

        cmd.execute(probe);

        assertEquals("Active Repairs         \n" +
                     "host1,host2,host3,host4\n", cmdOutput.toString());
    }
}

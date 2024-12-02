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

import java.util.Iterator;
import java.util.List;

public interface IAutoRepairTokenRangeSplitter
{

    /**
     * Split the token range you wish to repair into multiple assignments.
     * The autorepair framework will repair the list of returned subrange in a sequence.
     * @param repairType The type of repair being executed
     * @param primaryRangeOnly Whether to repair only this node's primary ranges or all of its ranges.
     * @param repairPlans A list of ordered prioritized repair plans to generate assignments for in order.
     * @return Iterator of repair assignments, with each element representing a grouping of repair assignments for a given keyspace.
     */
    Iterator<KeyspaceRepairAssignments> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, List<PrioritizedRepairPlan> repairPlans);
}

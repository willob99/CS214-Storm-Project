/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * This is an abstract class that should be extended for topologies with variable
 * numbers of bolts and spouts that are submitted by storm-adjuster.py.
 */

public abstract class AbstractVariableInstancesTopology extends ConfigurableTopology {

    protected String topoName;
    protected int numBoltsAndSpouts;
    protected ArrayList<Integer> numInstances;

    public AbstractVariableInstancesTopology(String[] args) throws Exception {
        numInstances = new ArrayList<Integer>();

        // Required arguments:
        // 0: Topology name
        // 1: Number of spouts and bolts
        // [2, a]: Number of instances for each spout and bolt
        // [a + 1, n]: Other arguments (optional) 

        if (args == null || args.length < 2) {
            // Not enough arguments provided.
            throw new Exception("Required arguments missing.");
        }
        topoName = args[0];
        numBoltsAndSpouts = Integer.parseInt(args[1]);
        if (args.length < 2 + numBoltsAndSpouts) {
            throw new Exception("Not enough arguments provided.");
        }
        for (int i = 2; i < args.length; i++) {
            numInstances.add(Integer.parseInt(args[i]));
        }
    }
}

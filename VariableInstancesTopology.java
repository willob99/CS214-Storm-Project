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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * This is a basic example of a Storm topology.
 */
public class VariableInstancesTopology extends ConfigurableTopology {
    public static final String baseDir = "/home/will/testout";

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new VariableInstancesTopology(), args);
    }

    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        int numBoltsAndSpouts = 4;
        ArrayList<Integer> numInstances = new ArrayList<Integer>();

        if (args != null && args.length > 0) {
            if (args.length != numBoltsAndSpouts) {
                System.out.println("wrong number of arguments, defaulting to all 1s");
                for (int i = 0; i < numBoltsAndSpouts; i++) {
                    numInstances.add(1);
                }
            }
            else {
                for (String num : args) {
                    numInstances.add(Integer.parseInt(num));
                }
            }
        }
        else {
            // If no args provided, default all to 1.
            for (int i = 0; i < numBoltsAndSpouts; i++) {
                numInstances.add(1);
            }
        }

        try {
            FileWriter fw = new FileWriter(baseDir.concat("/note.txt"), true);
            fw.write("creating spouts/bolts with these numbers of instances: ");
            for (int i : numInstances) {
                fw.write(String.format("%d ", i));
            }
            fw.write("\n");
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }

        builder.setSpout("word", new TestWordSpout(), numInstances.get(0));
        builder.setBolt("exclaim1", new ExclamationBolt(), numInstances.get(1)).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), numInstances.get(2)).shuffleGrouping("exclaim1");
        builder.setBolt("output", new WriteFileBolt(), numInstances.get(3)).shuffleGrouping("exclaim2");

        conf.setDebug(true);

        // Think about number of workers to use
        conf.setNumWorkers(3);

        return submit("dynamic-instances-test", conf, builder);
    }

    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class WriteFileBolt extends BaseRichBolt {
        OutputCollector collector;
        public static final String baseDir = "/home/will/testout";

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                FileWriter fw = new FileWriter(baseDir.concat("/out.txt"), true);
                fw.write(tuple.getString(0));
                fw.close();
            } catch (IOException e) {
                System.out.println(e);
            }
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declarer.declare(new Fields("word"));
        }

    }
}

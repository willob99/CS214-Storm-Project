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
import java.util.Random;

import org.apache.storm.starter.AbstractVariableInstancesTopology;
import org.apache.storm.starter.spout.TestRandomSentenceSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * This is an example of a Storm topology with variable numbers of instances of spouts and bolts.
 */
public class VariableInstancesTopology extends AbstractVariableInstancesTopology {
    public static final String baseDir = "/home/will/testout";

    public VariableInstancesTopology(String[] args) throws Exception {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new VariableInstancesTopology(args), args);
    }

    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // For testing purposes, log the number of instances in a file.
        try {
            FileWriter fw = new FileWriter(baseDir.concat("/note.txt"), true);
            fw.write("Creating spouts/bolts with these numbers of instances: ");
            for (int i : super.numInstances) {
                fw.write(String.format("%d ", i));
            }
            fw.write("\n");
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }

        if (super.numBoltsAndSpouts != 4) {
            throw new Exception("Number of spouts and bolts does not match the topology.");
        }

        // Construct topology with numbers of instances from superclass.
        // This spout always has 1 instance for testing purposes.
        builder.setSpout("word", new TestWordSpout(), 1);
        builder.setBolt("exclaim1", new ExclamationBolt(), super.numInstances.get(1)).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), super.numInstances.get(2)).shuffleGrouping("exclaim1");
        builder.setBolt("output", new WriteFileBolt(), super.numInstances.get(3)).shuffleGrouping("exclaim2");

        conf.setDebug(true);

        conf.setNumWorkers(3);

        return submit(super.topoName, conf, builder);
    }

    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            // Utils.sleep(100);
            Random rand = new Random(System.currentTimeMillis());
            int exp = (int)(rand.nextFloat() * 13);
            int stop = (int)Math.round(Math.pow(5, exp));
            String outputString = tuple.getString(0);
            for (int i = 0; i < stop; i++) {
                outputString = outputString.concat("!");
            }
            collector.emit(tuple, new Values(outputString));
            // collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
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
            System.out.println(tuple.getString(0));
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
        }

    }
}

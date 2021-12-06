# CS214-Storm-Project

### VariableInstancesTopology.java
This is an example of a topology with variable numbers of instances of its spouts and bolts. It extends the abstract class AbstractVariableInstancesTopology. It creates a topology with one spout and three bolts. The spout generates random words, the first two bolts append random numbers of exclamation points to it, and the final bolt prints the strings to an output file.

Put VariableInstancesTopology.java in \[storm directory\]/examples/storm-starter/src/jvm/org/apache/storm/starter/.
Fill in the baseDir variable in two places so the topology can write its output to a file.
Before you submit the topology, and every time you change it, run `mvn package` in examples/storm-starter/.

### AbstractVariableInstancesTopology.java
This abstract class extends Storm's ConfigurableTopology class and handles some of the details of implementing a topology with variable numbers of instances. It reads the arguments that storm-adjuster.py provides and saves them in variables that can be accessed from a subclass.

This file should also be saved in  \[storm directory\]/examples/storm-starter/src/jvm/org/apache/storm/starter/.

### storm-adjuster.py
This script is a wrapper for the Storm client that submits topologies to Storm, queries Storm for metrics on running topologies, and if the metrics meet certain criteria, kills the running topology, adjusts the numbers of instances of spouts and bolts, and resubmits the topology. Like Storm, it runs until it is killed.

This can go anywhere. Fill in the path variables in the file to point to the right places. When running this script, provide the name of the topology you want to run as an argument.

This file can be modified to implement new adjustment policies, if desired.

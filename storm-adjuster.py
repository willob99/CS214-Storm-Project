import subprocess

STORM_PY_LOC="/opt/apache-storm-2.3.0/bin/storm.py"
LOCAL_OR_JAR="local"
JAR_FILE_LOC="/opt/apache-storm-2.3.0/examples/storm-starter/target/storm-starter-2.3.0.jar"
TOPOLOGY="org.apache.storm.starter.VariableInstancesTopology"

NUM_BOLTS_AND_SPOUTS = 4
num_instances = [1] * NUM_BOLTS_AND_SPOUTS

cmd = ["sudo", "python3", STORM_PY_LOC, LOCAL_OR_JAR, JAR_FILE_LOC, TOPOLOGY]
cmd = cmd + list(map(lambda num: str(num), num_instances))

# Only when running locally
cmd.append("--local-ttl=60")

# Execute command
subprocess.run(cmd)

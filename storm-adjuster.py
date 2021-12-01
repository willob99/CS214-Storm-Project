from datetime import datetime
import subprocess
import requests
import json
import time

# Location of Python Storm client
STORM_PY_LOC="/opt/apache-storm-2.3.0/bin/storm.py"
# Should Storm be run in local mode (local) or regular cluster mode (jar)
LOCAL_OR_JAR="jar"
# Location of Storm jar file
JAR_FILE_LOC="/opt/apache-storm-2.3.0/examples/storm-starter/target/storm-starter-2.3.0.jar"
# Topology to submit
TOPOLOGY="org.apache.storm.starter.VariableInstancesTopology"

# How many instances to start with for each spout and bolt.
STARTING_INSTANCES=2
# Number of bolts and spouts in the topology.
NUM_BOLTS_AND_SPOUTS = 4
# List giving the number of instances for each spout and bolt.
# They all start out with the same number.
num_instances = [STARTING_INSTANCES] * NUM_BOLTS_AND_SPOUTS

# Build command to submit topology
cmd = ["sudo", "python3", STORM_PY_LOC, LOCAL_OR_JAR, JAR_FILE_LOC, TOPOLOGY]
# Generate unique topology name from timestamp.
now = datetime.now()
topology_name = "dynamic-instances-test-" + now.strftime("%m-%d-%Y-%H-%M-%S")
cmd.append(topology_name)
# Append numbers of instances to command, each as a separate element in the list.
cmd = cmd + list(map(lambda num: str(num), num_instances))
# When running locally, use this to make Storm run for a minute (instead of 20s default).
# cmd.append("--local-ttl=60")

# Execute command to submit initial topology.
subprocess.run(cmd)
print("Pausing while topology is submitted")
time.sleep(60)

# Run until killed, similar to a Storm topology.
while (1):
    # Send requests for metrics to Storm REST API.
    topology_info = requests.get("http://localhost:8080/api/v1/topology/summary").json()
    cluster_info = requests.get("http://localhost:8080/api/v1/cluster/summary").json()
    # For debugging
    print(topology_info)
    print(cluster_info)

    # Extract useful metrics.
    # requestedTotalMem = topology_info['topologies'][-1]['requestedTotalMem']
    assignedTotalMem = topology_info['topologies'][-1]['assignedTotalMem']
    # clusterTotalMem = cluster_info['totalMem']
    availMem = cluster_info['availMem']

    # mem_ratio = assignedTotalMem / requestedTotalMem
    mem_ratio = float(assignedTotalMem) / float(availMem)
    print("assignedTotalMem: ", assignedTotalMem)
    print("availMem: ", availMem)
    print("mem_ratio: ", mem_ratio)


    if (mem_ratio < 0.70 or mem_ratio > 0.90):
        # If mem ratio outside of ideal range, kill the topology.
        killcmd = ["sudo", "python3", STORM_PY_LOC, "kill", topology_name]
        subprocess.run(killcmd)
        time.sleep(5)
        print("killed topology")

        # Generate new numbers of instances (for now, keep it the same for all spouts and bolts).
        num_instances = [max(num_instances[0] - 1, 1) if mem_ratio > 0.90 else num_instances[0] + 1] * NUM_BOLTS_AND_SPOUTS
        print("num_instances: ", num_instances)

        cmd = ["sudo", "python3", STORM_PY_LOC, LOCAL_OR_JAR, JAR_FILE_LOC, TOPOLOGY]
        # Generate new topology name.
        now = datetime.now()
        topology_name = "dynamic-instances-test-" + now.strftime("%m-%d-%Y-%H-%M-%S")
        cmd.append(topology_name)
        # Submit topology with new numbers of instances.
        cmd = cmd + list(map(lambda num: str(num), num_instances))
        subprocess.run(cmd)

    # Wait until time to check again (in production, this will be much larger).
    time.sleep(60)
 

# Sample of metrics available
# topology_info: {'schedulerDisplayResource': False, 'topologies': [{'owner': 'root', 'assignedGenericResources': '', 'requestedCpu': 70.0, 'topologyVersion': None, 'replicationCount': 1, 'requestedGenericResources': '', 'stormVersion': '2.3.0', 'executorsTotal': 7, 'assignedMemOnHeap': 896.0, 'assignedTotalMem': 896.0, 'assignedCpu': 70.0, 'requestedMemOnHeap': 896.0, 'encodedId': 'dynamic-instances-test-3-1638338546', 'uptimeSeconds': 30, 'uptime': '30s', 'schedulerInfo': None, 'requestedTotalMem': 896.0, 'assignedMemOffHeap': 0.0, 'workersTotal': 3, 'requestedMemOffHeap': 0.0, 'name': 'dynamic-instances-test', 'id': 'dynamic-instances-test-3-1638338546', 'tasksTotal': 7, 'status': 'ACTIVE'}]}
# cluster_info: {'stormVersion': '2.3.0', 'fragmentedCpu': 0.0, 'executorsTotal': 7, 'totalMem': 4096.0, 'availCpu': 400.0, 'slotsTotal': 4, 'stormVersionInfo': {'date': '2021-09-23T20:39Z', 'srcChecksum': '4726872c58ee8b0a63454459a15af2', 'version': '2.3.0', 'branch': '(no branch)', 'user': 'ethanli', 'url': 'https://gitbox.apache.org/repos/asf/storm.git', 'revision': 'b5252eda18e76c4f42af58d7481ea66cf3ec8471'}, 'slotsUsed': 3, 'bugtracker-url': None, 'topologies': 1, 'totalCpu': 400.0, 'cpuAssignedPercentUtil': '0.000', 'availMem': 4096.0, 'slotsFree': 1, 'memAssignedPercentUtil': '0.000', 'availGenerics': '', 'totalGenerics': '', 'central-log-url': None, 'user': None, 'fragmentedMem': 0.0, 'tasksTotal': 7, 'schedulerDisplayResource': False, 'supervisors': 1}

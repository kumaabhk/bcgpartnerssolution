# bcgpartnerssolution
solution for BCP Partners test

#usgae
#. ./scripts/set_env.sh
#sh $TEST_ROOT/scripts/run.sh

created two context config, 
local_context to run in local environment
cluster_context for cluster mode ( this has configurable spark resources for running on cluster)

set_env.sh- initialize the current directory
run.sh - executable shell script using spark configs for resources
run.py- class initializing spark context and calling the transforms module
ratings.py- holding only the logic for transformations

Note: for current solution, have kept downlaoded data in pycharm data folder, but for huge volume i would assume data to be stored in HDFS (if on-premises)
and code needs to be changes to read data from HDFS

I have some issue on my personal laptop which is throwing following error:
pyspark.sql.utils.IllegalArgumentException: 'Unsupported class file major version 57'
py4j.protocol.Py4JJavaError: An error occurred while calling o153.collectToPython.
: java.lang.IllegalArgumentException: Unsupported class file major version 57





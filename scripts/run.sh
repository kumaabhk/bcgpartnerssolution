EXECUTOR_MEMORY=10G
MAX_EXECUTORS=4
MEMORY_OVERHEAD=10000
DRIVER_CORES=4
OFFHEAP_MEMORY=30G
DRIVER_MEMORY=50G
SHUFFLE_PARTITION=200
EXECUTOR_CORES=2

spark2/bin/spark2-submit --master yarn \
--deploy-mode cluster \
--executor-memory $EXECUTOR_MEMORY \
--executor-cores $EXECUTOR_CORES \
--conf spark.dynamicAllocation.enable=true \
--conf spark.executor.memory.overhead=$MEMORY_OVERHEAD \
--conf spark.driver.cores=$DRIVER_CORES \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=$OFFHEAP_MEMORY \
--driver-memory $DRIVER_MEMORY \
--conf spark.sql.shuffle.partitions=$SHUFFLE_PARTITION \
--py-files $build_dir/context.zip,$build_dir/transfors.zip \
$TEST_ROOT/scripts/run.py

#usgae
#. ./scripts/set_env.sh
#sh $TEST_ROOT/scripts/run.sh



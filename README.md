# Spark-PFP
A Python implementation of PFP and IncMiningPFP on Spark (v2.7.0). <br>
The original algorithms are based on MapReduce. <br>
Spark is an efficient big data engine that provides the MapReduce model. <br>
Spark RDD allows fast memory access while Hadoop MapReduce doesn not.

## Environment Setup
We use an [Alibaba Cloud ROS template](https://rosnext.console.aliyun.com/cn-qingdao/samples/Spark_Hadoop_Ecs_Instance_Group) to setup a Spark cluster on cloud. <br>
The cluster includes HDFS. <br>
In the parent directory of PFP and IncMiningPFP, `$ mkdir exp_output`. <br>
For IncMiningPFP, make a directory called `data` in the HDFS root directory.

## Prepare Datasets
We obtained the datasets (retail, kosarak, chainstore, RecordLink) from the [SPMF](http://www.philippe-fournier-viger.com/spmf/index.php?link=datasets.php) library. <br>
To generate incremental datasets from the original datasets, run `gen.sh` in `splitdb/`. <br>
It splits the original dataset into multiple incremental chunks of a fixed size. <br>
Put the datasets in `incdatasets/` directory. <br>
For PFP, put `incdatasets/` in `PFP/` directory. <br>
For IncMiningPFP, upload `incdatasets/` to `hdfs:///`.

## Run the Algorithms
1. Start Spark and HDFS.
2. Run `$ sh run.sh` at the master node.

`run.sh` specifies all the configurations including minimum supports, target datasets, incremental chunk sizes and number of executions. <br>
We use one core per node. <br>
We allocate 61G memory to the master node and 14G memory to each worker node.

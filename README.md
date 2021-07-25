# Spark-PFP
A Python implementation of PFP and IncMiningPFP on Spark (v2.7.0).

## Environment Setup
We use an Aliyun ROS template to setup a Spark cluster on cloud.
The cluster includes HDFS.
In the parent directory of PFP and IncMiningPFP, `$ mkdir exp_output`.
For IncMiningPFP, make a directory called `data` in the HDFS root directory.

## Prepare Datasets
We obtain the datasets from the [SPMF](http://www.philippe-fournier-viger.com/spmf/index.php?link=datasets.php) library.
To generate incremental datasets from the original datasets, run `gen.sh` in `splitdb/`.
It splits the original dataset into multiple incremental chunks of a fixed size.
Put the datasets in `incdatasets/` directory.
For PFP, put `incdatasets/` in `PFP/` directory.
For IncMiningPFP, upload `incdatasets/` to `hdfs:///`.

## Run the Algorithms
1. Start Spark and HDFS.
2. Run `$ sh run.sh` at the master node.
`run.sh` specifies all the configurations including minimum supports, incremental sizes and number of executions.
We use one core per node.
We allocate 61G memory to the master node and 14G memory to each worker node.
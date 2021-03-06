# DirectFileOutputCommitter for directly writing Avro files to S3 location in Spark

## Introduction

Currently, spark-avro uses default FileOutputCommitter for writing avro files irrespective of the filesystem it is writing to. FileOutputCommitter works by firstly writing the files to a temporary directory and then renaming them to the actual location. This behavior of FileOutputCommitter is required when speculative execution of mappers is enabled and the files are being written to HDFS location. In speculative execution of mappers, two (or more) mappers might try writing the same file split, thus the initial write location must differ among mappers.

## Direct avro file write to S3

When writing avro files into S3 in Spark, we don't need FileOutputCommitter's write behavior. Unlike HDFS which doesn't allow two (or more) writers trying to write to the same file, S3 allows multiple writers to write to the same file, and only one is allowed to succeed since the visibility is atomic. ~~Thus, it is totally safe for avro write to happen directly to the target S3 location.~~

The above line isn't true. If spark-speculation is enabled, there is a chance of data-loss with direct output commit if one of the speculative executors fail. More info here - https://issues.apache.org/jira/browse/SPARK-10063

Please look at the [Usage](#usage) to know when is it safe to use the direct output committer.


## Implementation Details

Two files have been added to support direct write of avro files: DirectFileOutputCommitter and DirectAvroKeyOutputFormat. The change has been tested with AvroSuite.scala that comes with spark-avro. Benchmarking results in local mode will be published soon.

## Usage

* Replace the spark-avro jar in spark driver's and executor's classpath
* Set `mapreduce.job.outputformat.class` to `com.databricks.spark.avro.DirectAvroKeyOutputFormat` in SparkSession's RuntimeConfig object, **OR** Add the following in mapred-site.xml
```
<property>
    <name>mapreduce.job.outputformat.class</name>
    <value>com.databricks.spark.avro.DirectAvroKeyOutputFormat</value>
</property>
```

### Use DirectFileOutputCommitter only when:
* The avro files are being written to S3 location, **AND**
* Both speculative-execution (spark.speculation) and append mode are disabled.

## Build Instructions

* Merge [spark-avro](https://github.com/databricks/spark-avro) branch with this branch.
* Run `build/sbt package` from project root
  * The new spark-avro jar will be generated under `./target/scala-2.11`

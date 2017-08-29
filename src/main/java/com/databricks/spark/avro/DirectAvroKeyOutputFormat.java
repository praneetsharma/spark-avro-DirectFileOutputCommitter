package com.databricks.spark.avro;

import java.io.IOException;

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * 
 * FileOutputFormat which uses DirectFileOutputCommitter
 * to write avro container files.
 * 
 * @author prsharma
 *
 * @param <T> The java type of the Avro data to write
 */
public class DirectAvroKeyOutputFormat<T> extends AvroKeyOutputFormat<T> {

	private FileOutputCommitter committer = null;
	
	@Override
	public synchronized 
	OutputCommitter getOutputCommitter(TaskAttemptContext context) 
		throws IOException {
		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new DirectFileOutputCommitter(output, context);
		}
		return committer;
	}

}

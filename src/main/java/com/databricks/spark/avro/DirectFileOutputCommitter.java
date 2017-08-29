/**
 * MIT License
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.databricks.spark.avro;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * Commits file to the target location directly, without going the
 * temporary directory route.
 * 
 * <br><br>
 * 
 * <p>Currently, spark-avro uses default FileOutputCommitter for writing avro files
 * irrespective of the filesystem it is writing to. FileOutputCommitter works by 
 * firstly writing the files to a temporary directory and then renaming them to 
 * the actual location. This behavior of FileOutputCommitter is required when 
 * speculative execution of mappers is enabled and the files are being written 
 * to HDFS location. In speculative execution of mappers, two (or more) mappers 
 * might try writing the same file split, thus the initial write location must 
 * differ among mappers.</p>
 * 
 * <p>When writing avro files into S3 in Spark, we don't need FileOutputCommitter's 
 * write behavior. Unlike HDFS which doesn't allow two (or more) writers trying 
 * to write to the same file, S3 allows multiple writers to write to the same 
 * file, and only one is allowed to succeed since the visibility is atomic.</p>
 * 
 * <b>Update: </b> Using Direct committer when "spark.speculation" is turned-on
 * is not recommended since it can result in data-loss in case a failure occurs.
 * More info can be found here - https://issues.apache.org/jira/browse/SPARK-10063
 * 
 * <h2> Usage </h2>
 * To use DirectFileOutputCommitter, do the following:
 * <ol>
 * 	<li>Add the updated spark-avro*.jar to Spark driver's and executor's classpath.</li>
 * 	<li>Now, we need to instruct Spark to use the new file output committer. 
 * 		To do that, do one of the following:
 * 		<ul>
 * 			<li>
 * 				Set "mapreduce.job.outputformat.class" to "com.databricks.spark.avro.DirectAvroKeyOutputFormat"
 * 				in SparkSession's RuntimeConfig object, <b>OR</b>
 * 			</li>
 * 			<li>
 * 				Add the following to mapred-site.xml:
 * 				<pre>{@code
 *  <property>
 *    <name>mapreduce.job.outputformat.class</name>
 *    <value>com.databricks.spark.avro.DirectAvroKeyOutputFormat</value>
 *  </property>
 *				}</pre>
 * 			</li>
 *		</ul>
 *	</li>
 * </ol>
 * 
 * @author Praneet Sharma
 */
public class DirectFileOutputCommitter extends FileOutputCommitter {

	private static final Log LOG = LogFactory.getLog(DirectFileOutputCommitter.class);

	private Path outputPath = null; // The path to which the avro task attempts are written
	private Path workPath = null; // Avro task attempt path; It is same as outputPath for Direct write

	public DirectFileOutputCommitter(Path path, JobContext jobContext) throws IOException {
		super(path, jobContext);
		if (path != null) {
			FileSystem fs = path.getFileSystem(jobContext.getConfiguration());
			this.outputPath = fs.makeQualified(path);
		}
		LOG.info("Using Direct avro write support.");
	}

	public DirectFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
		this(outputPath, (JobContext)context);		
		if (outputPath != null) {
			workPath = getTaskAttemptPath(context, outputPath);
		}
		LOG.info("Using Direct avro write support.");
	}

	/**
	 * @return The path to which the final target files are written directly.
	 */
	private Path getOutputPath() {
		return this.outputPath;
	}

	/**
	 * Get the directory that the task should write results into.
	 * @return the work directory
	 * @throws IOException
	 */
	@Override
	public Path getWorkPath() throws IOException {
		return this.workPath;
	}

	/**
	 * @return true if we have an output path set, else false.
	 */
	private boolean hasOutputPath() {
		return this.outputPath != null;
	}

	/**
	 * The path where the task attempt writes its output. For direct commit, 
	 * this path will be same as the output path.
	 * 
	 * @param context
	 */
	@Override
	public Path getTaskAttemptPath(TaskAttemptContext context) {
		return getOutputPath();
	}

	/**
	 * Returns the final output path as the task attempt path
	 * since output files should be written directly to the
	 * final path.
	 * 
	 * @param context
	 * @param out
	 * @return path
	 */
	public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
		return out;
	}

	/**
	 * There is not setup step since it is a direct commit.
	 *  
	 * @param context
	 */
	@Override
	public void setupJob(JobContext context) throws IOException {
		LOG.info("No setup required for avro write since the write is happening to the actual target location.");
	}

	/**
	 * If the output path is specified, create a _SUCCESS
	 * file only if the property mapreduce.fileoutputcommitter.marksuccessfuljobs
	 * is specified in the configuration.
	 * 
	 * @param context
	 */
	@Override
	public void commitJob(JobContext context) throws IOException {
		if (hasOutputPath()) {

			// No renaming or cleanup required since the write
			// happened to actual target location

			Path finalOutput = getOutputPath();
			FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());

			// True if the job requires output.dir marked on successful job.
			// Note that by default it is set to true.
			if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
				Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
				fs.create(markerPath).close();
			}
		} else {
			LOG.warn("Output Path is null in commitJob()");
		}
	}

	/**
	 * For direct commit, there is no clean-up step involved
	 * since the temporary location isn't created/used.
	 * 
	 * @param context
	 */
	@Override
	@Deprecated
	public void cleanupJob(JobContext context) throws IOException {
		LOG.info("No cleanup required in cleanupJob() for direct avro write.");
	}

	@Override
	public void abortJob(JobContext context, JobStatus.State state) 
			throws IOException {
		LOG.info("No cleanup required in abortJob() for direct avro write.");
	}

	/**
	 * Commit task is a NO-OP since we are performing direct commit.
	 * 
	 * @param context
	 */
	@Override
	public void commitTask(TaskAttemptContext context) 
			throws IOException {
		commitTask(context, null);
	}

	/**
	 * Commit task is a NO-OP since we are performing direct commit.
	 * 
	 * @param context
	 * @param taskAttemptPath
	 */
	public void commitTask(TaskAttemptContext context, Path taskAttemptPath) 
			throws IOException {
		LOG.info("No task to commit for direct avro write.");
	}

	/**
	 * Abort task is a NO-OP since we are performing direct commit.
	 * 
	 * @param context
	 */
	@Override
	public void abortTask(TaskAttemptContext context) throws IOException {
		abortTask(context, null);
	}

	/**
	 * Abort task is a NO-OP since we are performing direct commit.
	 * 
	 * @param context
	 * @param taskAttemptPath
	 */
	@Private
	public void abortTask(TaskAttemptContext context, Path taskAttemptPath) 
			throws IOException {
		LOG.info("No cleanup needed in abortTask() for direct avro write.");
	}

	/**
	 * Since direct write is happening, task-commit step isn't needed.
	 * 
	 * @param context
	 */
	@Override
	public boolean needsTaskCommit(TaskAttemptContext context)
			throws IOException {
		return needsTaskCommit(context, null);
	}

	/**
	 * Since direct write is happening, task-commit step isn't needed.
	 * 
	 * @param context
	 * @param taskAttemptPath
	 */
	public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath)
			throws IOException {
		return false;
	}
}

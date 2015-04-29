package co.vine.sre

import org.apache.hadoop.conf._
import org.apache.hadoop.util._
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.{NullWritable, Text}
import java.util

object Uniq extends Configured with Tool {
  def run(args: Array[String]): Int = {
  	val startTime = System.currentTimeMillis()

  	val inputPaths = args.slice(0, args.length-1).map(new Path(_))
  	val outputPath = new Path(args.last)

  	val job = new Job(getConf())
  	val dfs = FileSystem.get(job.getConfiguration())

    // determine number of reducers
    val totalInputSize = inputPaths.foldLeft(0L) {
    	_ + dfs.getContentSummary(_).getLength()
    }
    val reducerInputSize = 4294967296L // 4GB
    val numReducers = (totalInputSize/reducerInputSize)

    job.setJarByClass(Uniq.getClass)
    job.setMapperClass(classOf[UniqMapper])
    job.setReducerClass(classOf[UniqReducer])
    job.setCombinerClass(classOf[UniqReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NullWritable])
    job.setNumReduceTasks(numReducers.toInt)

    // add output/input paths
    FileOutputFormat.setOutputPath(job, outputPath)
    for (path <- inputPaths)
      FileInputFormat.addInputPath(job, path)

    val jobSuccess = job.waitForCompletion(true)
    printSummary(startTime, job.getCounters())

    if (jobSuccess) 0 else 1
  }

  def printSummary(startTime: Long, counters: Counters) : Unit = {
 	val inputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter",
                                            "MAP_INPUT_RECORDS").getValue()
 	val outputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter",
                                            "REDUCE_OUTPUT_RECORDS").getValue()

 	println("job run time: %d".format((System.currentTimeMillis-startTime)/1000))
 	println("duplicate records discarded: %d".format(inputRecords-outputRecords))
  }
}
package WordCountStringPair;
import java.io.IOException;
import java.util.StringTokenizer;

import mrcube.holistic.mr1estimate.StringPairMRCubeMR1GroupComparator;
import mrcube.holistic.mr1estimate.StringPairMRCubeMR1KeyComparator;
import mrcube.holistic.mr1estimate.StringPairMRCubeMR1Partitioner;
import mrcube.holistic.mr2materialize.stringmultiple.StringMultipleMRCubeMR2GroupComparator;
import mrcube.holistic.mr2materialize.stringmultiple.StringMultipleMRCubeMR2KeyComparator;
import mrcube.holistic.mr2materialize.stringmultiple.StringMultipleMRCubeMR2Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import datacube.common.StringPair;
import datacube.configuration.DataCubeParameter;

public class WordCountStringPair
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "wordcount_" + conf.get("dataset") + "_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(WordCountStringPair.class);
		
		job.setMapperClass(WordCountStringPairMapper.class);
		job.setReducerClass(WordCountStringPairReducer.class);
		
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(StringPairMRCubeMR1Partitioner.class);
		job.setSortComparatorClass(StringPairMRCubeMR1KeyComparator.class);
		job.setGroupingComparatorClass(StringPairMRCubeMR1GroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("wordcount.output.path");  

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
}

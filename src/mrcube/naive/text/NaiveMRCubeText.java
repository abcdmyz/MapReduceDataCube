package mrcube.naive.text;
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

public class NaiveMRCubeText
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "naive_mrcube_text_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(NaiveMRCubeText.class);
		
		job.setMapperClass(NaiveMRCubeTextMapper.class);
		job.setCombinerClass(NaiveMRCubeTextCombiner.class);
		job.setReducerClass(NaiveMRCubeTextReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(TextMRCubeNaivePartitioner.class);
		job.setSortComparatorClass(TextMRCubeNaiveKeyComparator.class);
		job.setGroupingComparatorClass(TextMRCubeNaiveGroupComparator.class);
   
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("naive.output.path");  
		
		System.out.println("naive input: " + inputPath);
		System.out.println("naive output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
}

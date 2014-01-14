package mrcube.naive;
import java.io.IOException;
import java.util.StringTokenizer;

import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.common.StringPair;
import mrcube.holistic.mr1estimate.StringPairGroupComparator;
import mrcube.holistic.mr1estimate.StringPairKeyComparator;
import mrcube.holistic.mr1estimate.StringPairPartitioner;
import mrcube.holistic.mr2materialize.StringMultipleGroupComparator;
import mrcube.holistic.mr2materialize.StringMultipleKeyComparator;
import mrcube.holistic.mr2materialize.StringMultiplePartitioner;

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

public class NaiveMRCube
{
	public void run(Configuration conf) throws Exception 
	{
		Job job = new Job(conf, "naive mrcube");
		job.setJarByClass(NaiveMRCube.class);
		
		job.setMapperClass(NaiveMRCubeMapper.class);
		job.setReducerClass(NaiveMRCubeReducer.class);
		
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(StringPairPartitioner.class);
		job.setSortComparatorClass(StringPairKeyComparator.class);
		job.setGroupingComparatorClass(StringPairGroupComparator.class);
    
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

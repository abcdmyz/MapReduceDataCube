package mrcube.holistic.mr1estimate;


import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.StringPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class HolisticMRCubeEstimate 
{
	public void run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "mrcube_m1");
		job.setJarByClass(HolisticMRCubeEstimate.class);
		
		job.setMapperClass(HolisticMRCubeEstimateMapper.class);
		//job.setCombinerClass(HolisticMRCubeEstimateReducer.class);
		job.setReducerClass(HolisticMRCubeEstimateReducer.class);
		
		job.setOutputKeyClass(StringPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(StringPairPartitioner.class);
		job.setSortComparatorClass(StringPairKeyComparator.class);
		job.setGroupingComparatorClass(StringPairGroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);

		/*
		InputFormat<IntWritable, Text> inputFormat = null;
		InputSampler.Sampler<IntWritable, Text> sampler =
				new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
		Object[] p = sampler.getSample(inputFormat, job);
		*/
		
		FileInputFormat.addInputPath(job, new Path(MRCubeParameter.MR1_INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(MRCubeParameter.MR1_OUTPUT_PATH));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

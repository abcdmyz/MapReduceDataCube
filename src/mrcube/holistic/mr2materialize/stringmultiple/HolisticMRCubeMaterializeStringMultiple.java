package mrcube.holistic.mr2materialize.stringmultiple;

import mrcube.holistic.mr1estimate.HolisticMRCubeEstimate;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimateMapper;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimateReducer;
import mrcube.holistic.mr1estimate.StringPairMRCubeMR1GroupComparator;
import mrcube.holistic.mr1estimate.StringPairMRCubeMR1KeyComparator;
import mrcube.holistic.mr1estimate.StringPairMRCubeMR1Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import datacube.common.StringMultiple;
import datacube.common.StringPair;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;

public class HolisticMRCubeMaterializeStringMultiple 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "mrcube_mr2_" + conf.get("total.tuple.size") + "_" + conf.get("percent.mem.usage");
		
		Job job = new Job(conf, jobName);
		
		job.setJarByClass(HolisticMRCubeMaterializeStringMultiple.class);
		
		job.setMapperClass(HolisticMRCubeMaterializeStringMultipleMapper.class);
		job.setReducerClass(HolisticMRCubeMaterializeStringMultipleReducer.class);
		
		job.setMapOutputKeyClass(StringMultiple.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(StringMultipleMRCubeMR2Partitioner.class);
		job.setSortComparatorClass(StringMultipleMRCubeMR2KeyComparator.class);
		job.setGroupingComparatorClass(StringMultipleMRCubeMR2GroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("mrcube.mr2.output.path");  
		
		System.out.println("mr2 input: " + inputPath);
		System.out.println("mr2 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		job.waitForCompletion(true);
	}


}

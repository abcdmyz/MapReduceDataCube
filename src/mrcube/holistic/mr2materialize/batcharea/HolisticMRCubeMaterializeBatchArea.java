package mrcube.holistic.mr2materialize.batcharea;

import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPair;
import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPairCombiner;
import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPairMapper;
import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPairReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import datacube.common.StringPair;
import datacube.common.StringPairMRCubeGroupComparator;
import datacube.common.StringPairMRCubeKeyComparator;
import datacube.common.StringPairMRCubePartitioner;

public class HolisticMRCubeMaterializeBatchArea 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "mrcube_mr2_batch_"  + conf.get("dataset") + "_" + conf.get("total.tuple.size") + "_" + conf.get("percent.mem.usage");
		
		Job job = new Job(conf, jobName);
		
		job.setJarByClass(HolisticMRCubeMaterializeBatchArea.class);
		
		job.setMapperClass(HolisticMRCubeMaterializeBatchAreaMapper.class);
		job.setCombinerClass(HolisticMRCubeMaterializeBatchAreaCombiner.class);
		job.setReducerClass(HolisticMRCubeMaterializeBatchAreaReducer.class);
		
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(StringPairMRCubePartitioner.class);
		job.setSortComparatorClass(StringPairMRCubeKeyComparator.class);
		job.setGroupingComparatorClass(StringPairMRCubeGroupComparator.class);
    
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

package tscube.holistic.mr1estimate.batchregion;

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

import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimate;
import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimateMapper;
import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimateReducer;
import datacube.common.StringPair;

public class HolisticTSCubeEstimateBatchRegion 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "tscube_mr1_batch_" + conf.get("dataset") + "_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTSCubeEstimate.class);
		
		job.setMapperClass(HolisticTSCubeEstimateBatchRegionMapper.class);
		job.setReducerClass(HolisticTSCubeEstimateBatchRegionReducer.class);

		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(StringPairMRCubeMR1Partitioner.class);
		job.setSortComparatorClass(StringPairMRCubeMR1KeyComparator.class);
		job.setGroupingComparatorClass(StringPairMRCubeMR1GroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);

		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("tscube.mr1.output.path");  
		
		System.out.println("mr1 input: " + inputPath);
		System.out.println("mr1 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}

}
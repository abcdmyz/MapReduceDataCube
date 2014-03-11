package tscube.holistic.mr1estimate.batchregion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimate;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringPairMRCubeGroupComparator;
import datacube.common.datastructure.StringPairMRCubeKeyComparator;
import datacube.common.datastructure.StringPairMRCubePartitioner;
import datacube.common.reducer.TSCubeMR1EstimateReducer;

public class HolisticTSCubeEstimateBatchRegionLong 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "tscube_mr1_batch_long_" + conf.get("dataset") + "_" + conf.get("total.tuple.size") + "_" + conf.get("datacube.measure");

		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTSCubeEstimateBatchRegionLong.class);

		job.setMapperClass(HolisticTSCubeEstimateBatchRegionLongMapper.class);
		job.setReducerClass(TSCubeMR1EstimateReducer.class);

		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(StringPairMRCubePartitioner.class);
		job.setSortComparatorClass(StringPairMRCubeKeyComparator.class);
		job.setGroupingComparatorClass(StringPairMRCubeGroupComparator.class);
    
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

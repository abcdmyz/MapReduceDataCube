package mrcube.holistic.mr1estimate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringPairMRCubeGroupComparator;
import datacube.common.datastructure.StringPairMRCubeKeyComparator;
import datacube.common.datastructure.StringPairMRCubePartitioner;

public class HolisticMRCubeEstimatePrintSample 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "mrcube_mr1_print_sample_" + conf.get("dataset") + "_" + conf.get("total.tuple.size") + "_" + conf.get("percent.mem.usage");
		
		Job job = new Job(conf, jobName);
		
		job.setJarByClass(HolisticMRCubeEstimate.class);
		
		job.setMapperClass(HolisticMRCubeEstimateMapper.class);
		job.setReducerClass(HolisticMRCubeEstimatePrintSampleReducer.class);

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
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("mrcube.mr1.output.path");  
		
		//System.out.println("mr1 input: " + inputPath);
		//System.out.println("mr1 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
}

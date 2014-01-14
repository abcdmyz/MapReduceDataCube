package mrcube.holistic.mr2materialize;

import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.common.StringMultiple;
import mrcube.holistic.common.StringPair;
import mrcube.holistic.common.Tuple;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimate;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimateMapper;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimateReducer;
import mrcube.holistic.mr1estimate.StringPairGroupComparator;
import mrcube.holistic.mr1estimate.StringPairKeyComparator;
import mrcube.holistic.mr1estimate.StringPairPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HolisticMRCubeMaterialize 
{
	public void run(Configuration conf) throws Exception 
	{
		Job job = new Job(conf, "mrcube_mr2");
		job.setJarByClass(HolisticMRCubeMaterialize.class);
		
		job.setMapperClass(HolisticMRCubeMaterializeMapper.class);
		job.setReducerClass(HolisticMRCubeMaterializeReducer.class);
		
		job.setMapOutputKeyClass(StringMultiple.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(StringMultiplePartitioner.class);
		job.setSortComparatorClass(StringMultipleKeyComparator.class);
		job.setGroupingComparatorClass(StringMultipleGroupComparator.class);
    
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

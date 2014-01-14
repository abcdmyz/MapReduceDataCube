package mrcube.holistic.mr3postprocess;

import mrcube.configuration.MRCubeParameter;

import mrcube.holistic.common.Tuple;
import mrcube.holistic.mr2materialize.HolisticMRCubeMaterialize;
import mrcube.holistic.mr2materialize.HolisticMRCubeMaterializeMapper;
import mrcube.holistic.mr2materialize.HolisticMRCubeMaterializeReducer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HolisticMRCubePostProcess 
{
	public void run(Configuration conf) throws Exception 
	{
		Job job = new Job(conf, "mrcube_mr3");
		job.setJarByClass(HolisticMRCubePostProcess.class);
		
		job.setMapperClass(HolisticMRCubePostProcessMapper.class);
		job.setCombinerClass(HolisticMRCubePostProcessReducer.class);
		job.setReducerClass(HolisticMRCubePostProcessReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("mrcube.mr2.output.path");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("mrcube.mr3.output.path");  

		System.out.println("mr3 input: " + inputPath);
		System.out.println("mr3 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileInputFormat.setInputPathFilter(job, HolisticMRCubePostProcessFilePathFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		
		job.waitForCompletion(true);
		
	}


}

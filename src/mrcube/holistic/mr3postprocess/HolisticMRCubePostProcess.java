package mrcube.holistic.mr3postprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import datacube.common.postprocess.DataCubePostProcessFilePathFilter;
import datacube.common.postprocess.DataCubePostProcessMapper;
import datacube.common.postprocess.DataCubePostProcessReducer;

public class HolisticMRCubePostProcess 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "mrcube_mr3_" + conf.get("dataset") + "_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);

		job.setJarByClass(HolisticMRCubePostProcess.class);
		
		job.setMapperClass(DataCubePostProcessMapper.class);
		job.setCombinerClass(DataCubePostProcessReducer.class);
		job.setReducerClass(DataCubePostProcessReducer.class);
		
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
		FileInputFormat.setInputPathFilter(job, DataCubePostProcessFilePathFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		
		job.waitForCompletion(true);
		
	}
}

package mrcube.reducerlearning;

import mrcube.naive.text.NaiveMRCubeText;
import mrcube.naive.text.NaiveMRCubeTextMapper;
import mrcube.naive.text.NaiveMRCubeTextReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReducerLearningMRCube 
{
	public void run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: mrcube <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "mrcube");
		job.setJarByClass(ReducerLearningMRCube.class);
		
		job.setMapperClass(ReducerLearningMRCubeMapper.class);
		//job.setCombinerClass(ReducerLearningMRCubeReducer.class);
		job.setReducerClass(ReducerLearningMRCubeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

package mrcube.naive;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaiveMRCube {
	
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
		job.setJarByClass(NaiveMRCube.class);
		
		job.setMapperClass(NaiveMRCubeMapper.class);
		job.setCombinerClass(NaiveMRCubeReducer.class);
		job.setReducerClass(NaiveMRCubeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(4);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

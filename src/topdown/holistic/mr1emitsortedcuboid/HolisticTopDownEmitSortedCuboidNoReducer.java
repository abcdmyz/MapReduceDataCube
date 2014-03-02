package topdown.holistic.mr1emitsortedcuboid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HolisticTopDownEmitSortedCuboidNoReducer 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "topdcube_mr1_noreducer_" + conf.get("dataset") + "_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTopDownEmitSortedCuboid.class);
		
		job.setMapperClass(HolisticTopDownEmitSortedCuboidMapper.class);
		//job.setCombinerClass(HolisticTopDownEmitSortedCuboidCombiner.class);
		//job.setReducerClass(HolisticTopDownEmitSortedCuboidReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);

		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("topdcube.mr1.output.path");  
		
		System.out.println("mr1 input: " + inputPath);
		System.out.println("mr1 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}

}

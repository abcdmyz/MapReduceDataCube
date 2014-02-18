package naive.holistic.hashuid;

import naive.holistic.text.NaiveMRCubeText;
import naive.holistic.text.NaiveMRCubeTextCombiner;
import naive.holistic.text.NaiveMRCubeTextMapper;
import naive.holistic.text.NaiveMRCubeTextReducer;
import naive.holistic.text.TextMRCubeNaiveGroupComparator;
import naive.holistic.text.TextMRCubeNaiveKeyComparator;
import naive.holistic.text.TextMRCubeNaivePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveMRCubeHashUid 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "naive_mrcube_hashuid_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(NaiveMRCubeHashUid.class);
		
		job.setMapperClass(NaiveMRCubeHashUidMapper.class);
		job.setCombinerClass(NaiveMRCubeHashUidCombiner.class);
		job.setReducerClass(NaiveMRCubeHashUidReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
   
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("naive.output.path");  
		
		System.out.println("naive input: " + inputPath);
		System.out.println("naive output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}

}

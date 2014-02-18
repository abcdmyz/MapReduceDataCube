package naive.holistic.batcharea;

import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcess;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcessFilePathFilter;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcessMapper;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcessReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveMRCubeBatchAreaSort 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "naive_batch_sort_" + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);

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
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("naive.mr1.output.path");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("naive.mr2.output.path");  
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileInputFormat.setInputPathFilter(job, HolisticMRCubePostProcessFilePathFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		
		job.waitForCompletion(true);
		
	}
}

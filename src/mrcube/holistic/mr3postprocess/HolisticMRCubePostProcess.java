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
	public void run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "mrcube_m3");
		job.setJarByClass(HolisticMRCubePostProcess.class);
		
		job.setMapperClass(HolisticMRCubePostProcessMapper.class);
		//job.setCombinerClass(HolisticMRCubeEstimateReducer.class);
		job.setReducerClass(HolisticMRCubePostProcessReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(MRCubeParameter.MR3_INPUT_PATH));
		FileInputFormat.setInputPathFilter(job, HolisticMRCubePostProcessFilePathFilter.class);

		FileOutputFormat.setOutputPath(job, new Path(MRCubeParameter.MR3_OUTPUT_PATH));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}

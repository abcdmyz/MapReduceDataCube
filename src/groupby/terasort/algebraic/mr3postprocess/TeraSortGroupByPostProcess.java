package groupby.terasort.algebraic.mr3postprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tscube.holistic.mr3postprocess.HolisticTSCubePostProcess;
import datacube.common.postprocess.DataCubePostProcessFilePathFilter;
import datacube.common.postprocess.DataCubePostProcessMapper;
import datacube.common.postprocess.DataCubePostProcessReducer;

public class TeraSortGroupByPostProcess 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "gbts_sp_mr3_" + conf.get("dataset") + "_" + conf.get("total.tuple.size") + "_" + conf.get("datacube.measure");
		
		Job job = new Job(conf, jobName);

		job.setJarByClass(TeraSortGroupByPostProcess.class);
		
		job.setMapperClass(DataCubePostProcessMapper.class);
		job.setCombinerClass(DataCubePostProcessReducer.class);
		job.setReducerClass(DataCubePostProcessReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("groupby.mr2.output.path");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("groupby.mr3.output.path");  

		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileInputFormat.setInputPathFilter(job, DataCubePostProcessFilePathFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		
		job.waitForCompletion(true);
		
	}

}

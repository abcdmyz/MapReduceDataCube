package topdown.holistic.mr2pipeline;

import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPair;
import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPairMapper;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcessFilePathFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringPairMRCubeGroupComparator;
import datacube.common.datastructure.StringPairMRCubeKeyComparator;
import datacube.common.datastructure.StringPairMRCubePartitioner;
import datacube.common.reducer.StringPairBatchAreaCombiner;
import datacube.common.reducer.StringPairBatchAreaReducer;

public class HolisticTopDownPipeline 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "topdcube_mr2_" + conf.get("dataset") + "_"  + conf.get("total.tuple.size");
		
		Job job = new Job(conf, jobName);
		
		job.setJarByClass(HolisticTopDownPipeline.class);
		
		job.setMapperClass(HolisticTopDownPipelineMapper.class);
		job.setCombinerClass(StringPairBatchAreaCombiner.class);
		job.setReducerClass(StringPairBatchAreaReducer.class);
		
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(StringPairMRCubePartitioner.class);
		job.setSortComparatorClass(StringPairMRCubeKeyComparator.class);
		job.setGroupingComparatorClass(StringPairMRCubeGroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("topdcube.mr1.output.path");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("topdcube.mr2.output.path");  
		
		System.out.println("mr2 input: " + inputPath);
		System.out.println("mr2 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileInputFormat.setInputPathFilter(job, HolisticMRCubePostProcessFilePathFilter.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		job.waitForCompletion(true);
	}
}

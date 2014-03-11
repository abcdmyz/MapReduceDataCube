package tscube.holistic.mr2materialize.batcharea;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterialize;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterializeMapper;
import datacube.common.datastructure.StringTriple;
import datacube.common.datastructure.StringTripleTSCubeGroupComparator;
import datacube.common.datastructure.StringTripleTSCubeKeyComparator;
import datacube.common.datastructure.StringTripleTSCubePartitioner;
import datacube.common.reducer.StringTripleBatchAreaCombiner;
import datacube.common.reducer.StringTripleBatchAreaReducer;

public class HolisticTSCubeMaterializeBatchAreaShort 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "tscube_mr2_batch_short_" + conf.get("total.interval.number") + "_" + conf.get("dataset") + "_"  + conf.get("total.tuple.size") + "_" + conf.get("datacube.measure");
		
		System.out.println("reducer number:" + Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
 		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTSCubeMaterializeBatchAreaShort.class);
		
		job.setMapperClass(HolisticTSCubeMaterializeBatchAreaShortMapper.class);
		job.setCombinerClass(StringTripleBatchAreaCombiner.class);
		job.setReducerClass(StringTripleBatchAreaReducer.class);

		job.setMapOutputKeyClass(StringTriple.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(StringTripleTSCubePartitioner.class);
		job.setSortComparatorClass(StringTripleTSCubeKeyComparator.class);
		job.setGroupingComparatorClass(StringTripleTSCubeGroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));

		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("tscube.mr2.output.path");  
		
		System.out.println("mr2 input: " + inputPath);
		System.out.println("mr2 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}


}

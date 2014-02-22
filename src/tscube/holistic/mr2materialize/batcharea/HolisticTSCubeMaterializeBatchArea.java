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
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterializeCombiner;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterializeMapper;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterializeReducer;
import datacube.common.StringTripple;
import datacube.common.StringTrippleTSCubeGroupComparator;
import datacube.common.StringTrippleTSCubeKeyComparator;
import datacube.common.StringTrippleTSCubePartitioner;

public class HolisticTSCubeMaterializeBatchArea 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "tscube_mr2_batch_" + conf.get("dataset") + "_"  + conf.get("total.tuple.size");
		
		System.out.println("reducer number:" + Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
 		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTSCubeMaterializeBatchArea.class);
		
		job.setMapperClass(HolisticTSCubeMaterializeBatchAreaMapper.class);
		job.setCombinerClass(HolisticTSCubeMaterializeBatchAreaCombiner.class);
		job.setReducerClass(HolisticTSCubeMaterializeBatchAreaReducer.class);

		job.setMapOutputKeyClass(StringTripple.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(StringTrippleTSCubePartitioner.class);
		job.setSortComparatorClass(StringTrippleTSCubeKeyComparator.class);
		job.setGroupingComparatorClass(StringTrippleTSCubeGroupComparator.class);
    
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

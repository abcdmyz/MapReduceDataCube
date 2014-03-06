package topdown.holistic.mr1estimatesortedcuboid.stringpair;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidText;
import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidTextCombiner;
import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidTextMapper;
import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidTextReducer;
import datacube.common.datastructure.BatchArea;
import datacube.common.datastructure.BatchAreaGenerator;
import datacube.common.datastructure.CubeLattice;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringPairMRCubeGroupComparator;
import datacube.common.datastructure.StringPairMRCubeKeyComparator;
import datacube.common.datastructure.StringPairMRCubePartitioner;
import datacube.common.datastructure.Tuple;
import datacube.common.reducer.StringPairBatchAreaCombiner;
import datacube.configuration.DataCubeParameter;

public class HolisticTopDownEmitSortedCuboidStringPair 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "topdcube_mr1_sp_" + conf.get("dataset") + "_" + conf.get("total.tuple.size") + "_" + conf.get("datacube.measure");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTopDownEmitSortedCuboidStringPair.class);
		
		job.setMapperClass(HolisticTopDownEmitSortedCuboidStringPairMapper.class);
		job.setCombinerClass(StringPairBatchAreaCombiner.class);
		job.setReducerClass(HolisticTopDownEmitSortedCuboidStringPairReducer.class);

		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setPartitionerClass(StringPairMRCubePartitioner.class);
		job.setSortComparatorClass(StringPairMRCubeKeyComparator.class);
		job.setGroupingComparatorClass(StringPairMRCubeGroupComparator.class);

		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("topdcube.mr1.output.path");  
		
		System.out.println("mr1 input: " + inputPath);
		System.out.println("mr1 output: " + outputPath);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}

}
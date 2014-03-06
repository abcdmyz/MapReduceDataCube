package topdown.holistic.mr1emitsortedcuboid.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimate;
import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimateMapper;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringPairMRCubeGroupComparator;
import datacube.common.datastructure.StringPairMRCubeKeyComparator;
import datacube.common.datastructure.StringPairMRCubePartitioner;

public class HolisticTopDownEmitSortedCuboidText 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "topdcube_mr1_text_" + conf.get("dataset") + "_" + conf.get("total.tuple.size") + "_" + conf.get("datacube.measure");
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(HolisticTopDownEmitSortedCuboidText.class);
		
		job.setMapperClass(HolisticTopDownEmitSortedCuboidTextMapper.class);
		job.setCombinerClass(HolisticTopDownEmitSortedCuboidTextCombiner.class);
		job.setReducerClass(HolisticTopDownEmitSortedCuboidTextReducer.class);

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

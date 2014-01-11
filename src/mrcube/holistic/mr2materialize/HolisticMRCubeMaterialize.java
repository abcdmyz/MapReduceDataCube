package mrcube.holistic.mr2materialize;

import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.StringPair;
import mrcube.holistic.StringTripple;
import mrcube.holistic.Tuple;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimate;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimateMapper;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimateReducer;
import mrcube.holistic.mr1estimate.StringPairGroupComparator;
import mrcube.holistic.mr1estimate.StringPairKeyComparator;
import mrcube.holistic.mr1estimate.StringPairPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HolisticMRCubeMaterialize 
{
	public void run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "mrcube_m2");
		job.setJarByClass(HolisticMRCubeMaterialize.class);
		
		job.setMapperClass(HolisticMRCubeMaterializeMapper.class);
		//job.setCombinerClass(HolisticMRCubeEstimateReducer.class);
		job.setReducerClass(HolisticMRCubeMaterializeReducer.class);
		
		job.setOutputKeyClass(StringTripple.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(StringTripplePartitioner.class);
		job.setSortComparatorClass(StringTrippleKeyComparator.class);
		job.setGroupingComparatorClass(StringTrippleGroupComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(MRCubeParameter.MR2_INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(MRCubeParameter.MR2_OUTPUT_PATH));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}

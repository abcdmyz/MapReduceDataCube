package groupby.terasort.algebraic.mr2materialize.stringpair;

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
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringPairGroupByKeyComparator;
import datacube.common.datastructure.StringPairGroupByPartitioner;
import datacube.common.datastructure.StringTriple;
import datacube.common.datastructure.StringTripleTSCubeGroupComparator;
import datacube.common.datastructure.StringTripleTSCubeKeyComparator;
import datacube.common.datastructure.StringTripleTSCubePartitioner;
import datacube.common.reducer.StringTripleNoBACombiner;
import datacube.common.reducer.StringTripleNoBAReducer;

public class TeraSortGroupByMaterializeStringPair 
{
	public void run(Configuration conf) throws Exception 
	{
		String jobName = "gbts_sp_mr2_" + conf.get("total.interval.number") + "_" + conf.get("dataset") + "_"  + conf.get("total.tuple.size") + "_" + conf.get("datacube.measure");

		System.out.println("reducer number:" + Integer.valueOf(conf.get("mapred.reduce.tasks")));
		
 		Job job = new Job(conf, jobName);
		job.setJarByClass(TeraSortGroupByMaterializeStringPair.class);
		
		job.setMapperClass(TeraSortGroupByMaterializeStringPairMapper.class);
		job.setCombinerClass(TeraSortGroupByMaterializeStringPairCombiner.class);
		job.setReducerClass(TeraSortGroupByMaterializeStringPairReducer.class);

		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(StringPairGroupByPartitioner.class);
		job.setSortComparatorClass(StringPairGroupByKeyComparator.class);
		job.setGroupingComparatorClass(StringPairGroupByKeyComparator.class);
    
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.valueOf(conf.get("mapred.reduce.tasks")));

		String inputPath = conf.get("hdfs.root.path") + conf.get("dataset") + conf.get("dataset.input.path") + conf.get("total.tuple.size");
		String outputPath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("groupby.mr2.output.path");  
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
}

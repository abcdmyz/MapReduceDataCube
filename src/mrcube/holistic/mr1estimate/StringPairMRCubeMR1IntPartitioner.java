package mrcube.holistic.mr1estimate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import datacube.common.StringPair;

public class StringPairMRCubeMR1IntPartitioner 
{
	public class StringPairMRCubeMR1Partitioner extends Partitioner<StringPair, IntWritable> 
	{

		@Override
		public int getPartition(StringPair key, IntWritable value, int numPartitions) 
		{
			// TODO Auto-generated method stub
			return Math.abs(Integer.valueOf(key.getFirstString())) % numPartitions;
		}
		

	}


}

package tscube.holistic.mr1estimate;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import datacube.common.StringMultiple;

public class StringMultipleTSCubeMR1Partitioner extends Partitioner<StringMultiple, IntWritable> 
{

	@Override
	public int getPartition(StringMultiple key, IntWritable value, int numPartitions) 
	{
		String keyString = key.getString(0);
		
		return Math.abs(keyString.hashCode()) % numPartitions;
	}
}

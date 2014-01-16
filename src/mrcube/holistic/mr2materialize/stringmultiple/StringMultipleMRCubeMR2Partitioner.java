package mrcube.holistic.mr2materialize.stringmultiple;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import datacube.common.StringMultiple;

public class StringMultipleMRCubeMR2Partitioner extends Partitioner<StringMultiple, IntWritable> 
{

	@Override
	public int getPartition(StringMultiple key, IntWritable value, int numPartitions) 
	{
		String keyString = new String();
		
		for (int i = 0; i < key.getSize() - 1; i++)
		{
			keyString += key.getString(i);
		}
		
		return Math.abs(keyString.hashCode()) % numPartitions;
	}
}

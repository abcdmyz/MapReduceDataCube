package mrcube.holistic.mr2materialize;

import mrcube.holistic.common.StringMultiple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StringMultiplePartitioner extends Partitioner<StringMultiple, IntWritable> 
{

	@Override
	public int getPartition(StringMultiple key, IntWritable value, int numPartitions) 
	{
		String keyString = new String();
		
		for (int i = 0; i < key.getSize() - 1; i++)
		{
			keyString += key.getString(i);
		}
		
		return Math.abs(key.hashCode()) % numPartitions;
	}
}

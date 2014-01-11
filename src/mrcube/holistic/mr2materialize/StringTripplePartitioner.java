package mrcube.holistic.mr2materialize;

import java.util.prefs.InvalidPreferencesFormatException;

import mrcube.holistic.StringPair;
import mrcube.holistic.StringTripple;
import mrcube.holistic.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StringTripplePartitioner extends Partitioner<StringTripple, IntWritable> 
{

	@Override
	public int getPartition(StringTripple key, IntWritable value, int numPartitions) 
	{
		String keyString = key.getFirstString() + key.getSecondString();
		return Math.abs(key.hashCode()) % numPartitions;
	}
}

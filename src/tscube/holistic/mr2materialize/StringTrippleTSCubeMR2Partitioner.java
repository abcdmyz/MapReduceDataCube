package tscube.holistic.mr2materialize;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import datacube.common.StringPair;
import datacube.common.StringTripple;

public class StringTrippleTSCubeMR2Partitioner extends Partitioner<StringTripple, IntWritable> 
{

	@Override
	public int getPartition(StringTripple key, IntWritable value, int numPartitions) 
	{
		// TODO Auto-generated method stub
		return Math.abs(Integer.valueOf(key.getThirdString())) % numPartitions;
	}
	

}

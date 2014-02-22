package datacube.common;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StringTrippleTSCubePartitioner extends Partitioner<StringTripple, IntWritable> 
{

	@Override
	public int getPartition(StringTripple key, IntWritable value, int numPartitions) 
	{
		// TODO Auto-generated method stub
		return Math.abs(Integer.valueOf(key.getThirdString())) % numPartitions;
	}
	

}

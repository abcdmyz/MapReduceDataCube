package datacube.common.datastructure;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StringTripleTSCubePartitioner extends Partitioner<StringTriple, IntWritable> 
{

	@Override
	public int getPartition(StringTriple key, IntWritable value, int numPartitions) 
	{
		// TODO Auto-generated method stub
		return Math.abs(Integer.valueOf(key.getThirdString())) % numPartitions;
	}
	

}

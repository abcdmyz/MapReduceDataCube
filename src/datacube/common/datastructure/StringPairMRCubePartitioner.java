package datacube.common.datastructure;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StringPairMRCubePartitioner extends Partitioner<StringPair, IntWritable> 
{

	@Override
	public int getPartition(StringPair key, IntWritable value, int numPartitions) 
	{
		// TODO Auto-generated method stub
		return Math.abs(key.getFirstString().hashCode()) % numPartitions;
	}
	

}

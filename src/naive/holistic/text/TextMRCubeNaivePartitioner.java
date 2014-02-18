package naive.holistic.text;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextMRCubeNaivePartitioner extends Partitioner<Text, IntWritable> 
{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) 
	{
		Text p1 = (Text)key;
		
		String split1[] = p1.toString().split("//|");
		
		String p11 = split1[0] + "|" + split1[1] + "|";

		return Math.abs(p11.hashCode()) % numPartitions;
	}
	

}

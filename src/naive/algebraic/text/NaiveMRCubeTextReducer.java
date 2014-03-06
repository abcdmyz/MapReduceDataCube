package naive.algebraic.text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NaiveMRCubeTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int count = 0;
		
		for (IntWritable val : values) 
	    {
			count += val.get();
	    }	
		
		IntWritable countW = new IntWritable(count);
		context.write(key, countW);
	}
}
package naive.holistic.hashuid;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NaiveMRCubeHashUidCombiner extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		calculationGroupBy(key, values, context);
		
		//justPrintKeyValue(key, values, context);
	}
	
	private void justPrintKeyValue(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		for (IntWritable val : values) 
	    {
			context.write(key, val);
	    }
	}

	private void calculationGroupBy(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int last = -1;
	
		for (IntWritable val : values) 
	    {	
			if (val.get()!=last)
			{
				context.write(key, val);
			}

			last = val.get();
	    }	
	}
}
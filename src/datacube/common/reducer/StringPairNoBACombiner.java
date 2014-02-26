package datacube.common.reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;

public class StringPairNoBACombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> 
{
	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		
		if (conf.get("datacube.measure").equals("distinct"))
		{
			calculationDistinctGroupBy(key, values, context);
		}
		else if (conf.get("datacube.measure").equals("count"))
		{
			calculationCountGroupBy(key, values, context);
		}
		else  
		{
			//null
		}
		
		//justPrintKeyValue(key, values, context);
	}
	
	private void justPrintKeyValue(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		for (IntWritable val : values) 
	    {
			context.write(key, val);
	    }
	}

	private void calculationDistinctGroupBy(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String last = null;
	
		for (IntWritable val : values) 
	    {
			if (!key.getSecondString().equals(last))
			{
				context.write(key, val);
			}

			last = key.getSecondString();
	    }	
	}
	
	private void calculationCountGroupBy(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int count = 0;
	
		for (IntWritable val : values) 
	    {
			count++;
	    }	
		
		IntWritable countW = new IntWritable(count);
		context.write(key, countW);
	}
}
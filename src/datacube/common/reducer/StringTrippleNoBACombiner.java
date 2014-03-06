package datacube.common.reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringTriple;

public class StringTrippleNoBACombiner extends Reducer<StringTriple, IntWritable, StringTriple, IntWritable> 
{
	@Override
	public void reduce(StringTriple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
	
	private void justPrintKeyValue(StringTriple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		for (IntWritable val : values) 
	    {
			context.write(key, val);
	    }
	}

	private void calculationDistinctGroupBy(StringTriple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String lastF = null;
		String lastS = null;
		String lastT = null;
		int lastV = -1;
		
		for (IntWritable val : values) 
	    {
			if (!key.getThirdString().equals(lastT) || !key.getSecondString().equals(lastS) || !key.getFirstString().equals(lastF) || val.get() != lastV)
			{
				context.write(key, val);
			}

			lastS = key.getSecondString();
			lastF = key.getFirstString();
			lastT = key.getThirdString();
			lastV = val.get();
	    }
	}
	
	private void calculationCountGroupBy(StringTriple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
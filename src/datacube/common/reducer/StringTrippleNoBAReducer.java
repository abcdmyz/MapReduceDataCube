package datacube.common.reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringTripple;

public class StringTrippleNoBAReducer extends Reducer<StringTripple, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
	
	private void justPrintKeyValue(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
	
		for (IntWritable val : values) 
	    {
			outputKey.set(key.getFirstString().toString());
			
			context.write(outputKey, val);
	    }
		
		outputKey.set("********************");
		IntWritable outputValue = new IntWritable(1);
		context.write(outputKey, outputValue);
	}

	private void calculationDistinctGroupBy(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String last = null;
		int count = 0;

		for (IntWritable val : values) 
	    {
			if (!key.getSecondString().equals(last))
			{
				count++;
			}

			last = key.getSecondString();
	    }	
		
		Text outputKey = new Text();
		String splitString[] = key.getFirstString().split("\\|");
		outputKey.set(splitString[0] + "|" + splitString[1] + "|");
		
		IntWritable outputValue = new IntWritable(count);
		context.write(outputKey, outputValue);
	}
	
	private void calculationCountGroupBy(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		IntWritable countW = new IntWritable(10);
		int count = 0;	
	
		for (IntWritable val : values) 
	    {
			count += val.get();
	    }	
		
		Text outputKey = new Text();
		String splitString[] = key.getFirstString().split("\\|");
		outputKey.set(splitString[0] + "|" + splitString[1] + "|");
		
		countW.set(count);
		context.write(outputKey, countW);
	}
}
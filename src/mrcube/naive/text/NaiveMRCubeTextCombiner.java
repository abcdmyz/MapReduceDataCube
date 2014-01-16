package mrcube.naive.text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;

public class NaiveMRCubeTextCombiner extends Reducer<Text, IntWritable, Text, IntWritable> 
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
		String last = null;
	
		for (IntWritable val : values) 
	    {
			String keySplit[] = key.toString().split("\\|");
			
			if (!keySplit[2].equals(last))
			{
				context.write(key, val);
			}

			last = keySplit[2];
	    }	
	}
}
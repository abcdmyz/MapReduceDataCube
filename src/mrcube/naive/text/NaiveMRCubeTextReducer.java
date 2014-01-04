package mrcube.naive.text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NaiveMRCubeTextReducer extends Reducer<Text,Text,Text,Text> 
{
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		long sum = 0;
	   for (Text val : values) 
	    {
			String valueString = val.toString();
			String valueSplit[] = valueString.split(" ");
		
			String measure = valueSplit[valueSplit.length-1];
			
		   sum += Long.parseLong(measure);
		}
	   
	   result.set(String.valueOf(sum));
	   context.write(key, result);
	}
	
	
}


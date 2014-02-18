package naive.holistic.text;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;

public class NaiveMRCubeTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		calculationGroupBy(key, values, context);
		
		//justPrintKeyValue(key, values, context);
	}
	
	private void justPrintKeyValue(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
	
		for (IntWritable val : values) 
	    {
			String keySplit[] = key.toString().split("\\|");
			outputKey.set(keySplit[0] + "|" + keySplit[1] + "|");
			
			context.write(outputKey, val);
	    }
		outputKey.set("end");
		
		context.write(outputKey, one);
	}

	private void calculationGroupBy(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String last = null;
		int count = 0;

		for (IntWritable val : values) 
	    {
			String keySplit[] = key.toString().split("\\|");
			
			if (!keySplit[2].equals(last))
			{
				count++;
			}

			last = keySplit[2];
	    }	

		Text outputKey = new Text();
		String keySplit[] = key.toString().split("\\|");
		
		outputKey.set(keySplit[0] + "|" + keySplit[1] + "|");
		IntWritable outputValue = new IntWritable(count);
		context.write(outputKey, outputValue);
	}
}
package topdown.holistic.mr1estimatesortedcuboid.stringpair;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;

public class HolisticTopDownEmitSortedCuboidStringPairReducer extends Reducer<StringPair, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		String keyFirstSplit[] = key.getFirstString().split("\\|");
		String attributeSplit[] = keyFirstSplit[0].split(" ");
		
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
	
	/*
	private void justPrintKeyValue(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		for (IntWritable val : values) 
	    {
			context.write(key, val);
	    }
	}
	*/

	private void calculationDistinctGroupBy(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String lastS = null;
		int lastV = -1;
		
		Text outputKey = new Text();
		IntWritable one = new IntWritable(1);
		
		String[] publicSplit = key.getFirstString().split("\\|");
		
		for (IntWritable val : values) 
	    {
			if (!key.getSecondString().equals(lastS) ||  val.get() != lastV)
			{
	
				outputKey.set(publicSplit[0] + "|" + publicSplit[1] + " " + key.getSecondString() + "|" + String.valueOf(val) + "|");
				context.write(outputKey, one);
			}

			lastS = key.getSecondString();
			lastV = val.get();
	    }		
	}
	
	private void calculationCountGroupBy(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String lastS = null;
		
		IntWritable outputValue = new IntWritable();
		Text outputKey = new Text();
		
		String[] publicSplit = key.getFirstString().split("\\|");
		
		int count = 0;
		boolean first = true;
		
		for (IntWritable val : values) 
	    {
			if (first || key.getSecondString().equals(lastS) )
			{
				count += val.get();
			}
			else
			{
				outputValue.set(count);
				count = val.get();
				
				outputKey.set(publicSplit[0] + "|" + publicSplit[1] + " " + key.getSecondString() + "|");
				context.write(outputKey, outputValue);
			}

			lastS = key.getSecondString();
			first = false;
	    }
		
		outputValue.set(count);
		outputKey.set(publicSplit[0] + "|" + publicSplit[1] + " " + key.getSecondString() + "|");
		
		context.write(outputKey, outputValue);
	}
}
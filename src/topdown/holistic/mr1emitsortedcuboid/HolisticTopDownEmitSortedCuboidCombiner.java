package topdown.holistic.mr1emitsortedcuboid;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;

public class HolisticTopDownEmitSortedCuboidCombiner extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	private Configuration conf;

	@Override
	public void setup(Context context) throws IOException
	{
		conf = context.getConfiguration();
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
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
			
		}
	}
	
	private void calculationDistinctGroupBy(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		IntWritable outputValue = new IntWritable(1);
		context.write(key, outputValue);
	}
	
	
	private void calculationCountGroupBy(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
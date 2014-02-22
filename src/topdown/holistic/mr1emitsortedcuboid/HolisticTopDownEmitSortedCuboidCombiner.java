package topdown.holistic.mr1emitsortedcuboid;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HolisticTopDownEmitSortedCuboidCombiner extends Reducer<Text,IntWritable,Text,Text> 
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
		justPrintKeyOnce(key, values, context);
	}
	
	private void justPrintKeyOnce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputValue = new Text();
		context.write(key, outputValue);
	}
}
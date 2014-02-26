package tscube.holistic.mr2materialize.stringtripple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringMultiple;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringTripple;

public class HolisticTSCubeMaterializeReducer extends Reducer<StringTripple, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		groupByCalculation(key, values, context);
		
		//justOutput(key, values, context);
	}
	
	private void groupByCalculation(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		int count = 0;
		String last = null;
		
		for(IntWritable val : values)
		{
			if (!key.getSecondString().equals(last))
			{
				count++;
			}
			last = key.getSecondString();
		}
		
		outputKey.set(key.getFirstString());
			
		IntWritable outputValue = new IntWritable(count);
		context.write(outputKey, outputValue);
	}
	
	private void justOutput(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		int count = 0;
		String last = null;
		
		for(IntWritable val : values)
		{
			outputKey.set(key.getFirstString() + "--" + key.getSecondString() + "--" + key.getThirdString());
			
			context.write(outputKey, one);
		}
		outputKey.set("end");
		context.write(outputKey, one);
	}
}

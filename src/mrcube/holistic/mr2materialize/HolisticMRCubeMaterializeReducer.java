package mrcube.holistic.mr2materialize;

import java.io.IOException;

import mrcube.holistic.StringPair;
import mrcube.holistic.StringTripple;
import mrcube.holistic.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HolisticMRCubeMaterializeReducer extends Reducer<StringTripple, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	public void reduce(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		int count = 0;
		String last = null;
		
		for(IntWritable val : values)
		{
			if (key.getSecondString() != last)
			{
				count++;
			}
			last = key.getThirdString();
		}
		
		outputKey.set(key.getFirstString() + " " +  key.getSecondString());
		IntWritable outputValue = new IntWritable(count);
		
		context.write(outputKey, outputValue);
	}
}

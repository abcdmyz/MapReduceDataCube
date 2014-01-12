package mrcube.holistic.mr2materialize;

import java.io.IOException;

import mrcube.holistic.common.StringMultiple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//StringMultiple
public class HolisticMRCubeMaterializeReducer extends Reducer<StringMultiple, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringMultiple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		int count = 0;
		String last = null;
		
		for(IntWritable val : values)
		{
			if (key.getString(3) != last)
			{
				count++;
			}
			last = key.getString(3);
		}
		
		String outputString = new String();
		for (int i = 0; i < key.getSize() - 2; i++)
		{
			outputString += key.getString(i) + "|";
		}
		outputKey.set(outputString);
			
		IntWritable outputValue = new IntWritable(count);
		context.write(outputKey, outputValue);
	}
}

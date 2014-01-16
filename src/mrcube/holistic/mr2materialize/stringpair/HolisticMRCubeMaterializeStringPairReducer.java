package mrcube.holistic.mr2materialize.stringpair;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringMultiple;
import datacube.common.StringPair;

//StringMultiple
public class HolisticMRCubeMaterializeStringPairReducer extends Reducer<StringPair, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
		
		String keySplit[] = key.getFirstString().split("\\|");
		String outputString = new String();
		for (int i = 0; i < keySplit.length - 1; i++)
		{
			outputString += keySplit[i] + "|";
		}
		outputKey.set(outputString);
		//outputKey.set(key.getFirstString());
			
		IntWritable outputValue = new IntWritable(count);
		context.write(outputKey, outputValue);
	}
}

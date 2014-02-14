package tscube.holistic.mr2materialize;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;
import datacube.common.StringTripple;

public class HolisticTSCubeMaterializeCombiner extends Reducer<StringTripple, IntWritable, StringTripple, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String last = null;
		
		for (IntWritable val : values) 
	    {
			if (!key.getSecondString().equals(last))
			{
				context.write(key, val);
			}

			last = key.getSecondString();
	    }	
	}
}

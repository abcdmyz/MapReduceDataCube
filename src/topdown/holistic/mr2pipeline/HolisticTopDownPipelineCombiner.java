package topdown.holistic.mr2pipeline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;

public class HolisticTopDownPipelineCombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String lastF = null;
		String lastS = null;
		int lastV = -1;
		
		for (IntWritable val : values) 
	    {
			if (!key.getSecondString().equals(lastS) || !key.getFirstString().equals(lastF) || val.get() != lastV)
			{
				context.write(key, val);
			}

			lastS = key.getSecondString();
			lastF = key.getFirstString();
			lastV = val.get();
	    }	
	}
}

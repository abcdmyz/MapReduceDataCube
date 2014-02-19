package tscube.holistic.mr2materialize.batcharea;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;
import datacube.common.StringTripple;

public class HolisticTSCubeMaterializeBatchAreaCombiner extends Reducer<StringTripple, IntWritable, StringTripple, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String lastF = null;
		String lastS = null;
		String lastT = null;
		int lastV = -1;

		for (IntWritable val : values) 
	    {
			if (!key.getSecondString().equals(lastS) || !key.getFirstString().equals(lastF) || 
					!key.getThirdString().equals(lastT) || val.get() != lastV)
			{
				context.write(key, val);
			}

			lastS = key.getSecondString();
			lastF = key.getFirstString();
			lastT = key.getThirdString();
			lastV = val.get();
	    }	
	}
}

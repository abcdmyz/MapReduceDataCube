package mrcube.holistic.mr2materialize.stringpair;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;

public class HolisticMRCubeMaterializeStringPairCombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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

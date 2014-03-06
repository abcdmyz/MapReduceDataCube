package groupby.terasort.algebraic.mr2materialize.stringpair;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;

public class TeraSortGroupByMaterializeStringPairCombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> 
{
	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
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
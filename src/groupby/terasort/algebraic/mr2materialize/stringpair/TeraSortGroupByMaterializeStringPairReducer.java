package groupby.terasort.algebraic.mr2materialize.stringpair;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;

public class TeraSortGroupByMaterializeStringPairReducer extends Reducer<StringPair, IntWritable, Text, IntWritable> 
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
		Text outputKey = new Text();
		outputKey.set(key.getFirstString());
		
		context.write(outputKey, countW);
	}
}
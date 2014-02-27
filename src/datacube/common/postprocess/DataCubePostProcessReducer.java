package datacube.common.postprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DataCubePostProcessReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws  IOException, InterruptedException
	{
		Text outputKey = new Text();
		int count = 0;
		String last = null;

		for(IntWritable val : values)
		{
			count += val.get();
		}

		IntWritable outputValue = new IntWritable(count);
		context.write(key, outputValue);
	}
}

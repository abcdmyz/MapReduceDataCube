package naive.holistic.hashuid;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.configuration.DataCubeParameter;

public class NaiveMRCubeHashUidReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private Configuration conf;
	
	@Override
	public void setup(Context context)
	{
		conf = context.getConfiguration();
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		calculationGroupBy(key, values, context);
		
		//justPrintKeyValue(key, values, context);
	}
	
	private void justPrintKeyValue(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
	
		for (IntWritable val : values) 
	    {
			context.write(key, val);
	    }
		outputKey.set("end");
	}

	private void calculationGroupBy(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String last = null;
		int count = 0;
		int totalUidSize = Integer.valueOf(conf.get("total.uid.size"));
		boolean uidHash[] = new boolean[totalUidSize];
		int uid;

		for (IntWritable val : values) 
	    {
			uid = val.get();
			
			if (uidHash[uid] == false)
			{
				count++;
			}
			uidHash[uid] = true;
	    }	
		
		IntWritable outputValue = new IntWritable(count);
		context.write(key, outputValue);
	}
}
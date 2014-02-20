package mrcube.holistic.mr3postprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HolisticMRCubePostProcessMapper extends Mapper<Object, Text, Text, IntWritable>  
{
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] tupleSplit = value.toString().split("\t");
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();
		
		outputKey.set(tupleSplit[0]);
		outputValue.set(Integer.valueOf(tupleSplit[1]));
		context.write(outputKey, outputValue);
	}
}

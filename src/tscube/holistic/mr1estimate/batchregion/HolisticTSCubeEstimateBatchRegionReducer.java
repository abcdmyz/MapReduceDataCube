package tscube.holistic.mr1estimate.batchregion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;
import datacube.configuration.DataCubeParameter;

public class HolisticTSCubeEstimateBatchRegionReducer extends Reducer<StringPair,IntWritable,Text,Text> 
{
	private Configuration config;
	private double r = 0;
	
	private Configuration conf;

	@Override
	public void setup(Context context) throws IOException
	{
		conf = context.getConfiguration();
	}
	
	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		chooseBoundaryKey(key, values, context);
		
		//justPrintKeyValue(key, values, context);
	}
	
	private void justPrintKeyValue(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		Text outputValue = new Text();
	
		for (IntWritable val : values) 
	    {
			outputKey.set(key.getFirstString().toString());
			outputValue.set(key.getSecondString().toString());
			
			context.write(outputKey, outputValue);
	    }
	}

	private void chooseBoundaryKey(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		Text outputValue = new Text();

		/*
		long totalSampleSize = DataCubeParameter.getMRCubeTotalSampleSize(Long.valueOf(conf.get("total.tuple.size")));
		double sampleBias = Double.valueOf(conf.get("tscube.sample.bias"));
		int batchRegionNumber = DataCubeParameter.getTSCubeBatchRegionNumber(conf.get("dataset"));
		long totalTupleSize = (long) (totalSampleSize * sampleBias * batchRegionNumber);
		*/
		
		int totalTupleSize = 0;
		int intervalNumber = Integer.valueOf(conf.get("total.interval.number"));
		
		String allSample[] = new String[Integer.valueOf(conf.get("tscube.max.sample.size"))];
	
		for (IntWritable val : values) 
	    {
			allSample[totalTupleSize++] = key.getSecondString();
	    }
		
		int interval = (int) (totalTupleSize / (intervalNumber));
		System.out.println("boudary:" + totalTupleSize + " " + intervalNumber + " " + interval);
		int id = 1;
		int count = 0;
		
		while (id < intervalNumber)
		{
			count += interval;
			
			outputKey.set(allSample[count]);
			outputValue.set(String.valueOf(id));
			
			context.write(outputKey, outputValue);
			
			id++;
		}
	}

	@Override
	public void cleanup(Context context) throws IOException
	{

	}
}
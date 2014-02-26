package tscube.holistic.mr1estimate.allregion;

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

public class HolisticTSCubeEstimateReducer extends Reducer<StringPair,IntWritable,Text,Text> 
{
	private Text one = new Text("1");
	private Text two = new Text("2");
	
	private Configuration config;
	private FileSystem fs;
	private Path path;
	private FSDataOutputStream out;
	
	private double heapMemAvail = 0;
	private double maxTupleByReducer = 0;
	private double r = 0;
	
	private Configuration conf;
	private long maxReducerLimitByte;
	private double percentMemUsage;
	private int oneTupleSizeByByte;
	private long totalTupleSize;
	private long totalSampleSize;
	
	@Override
	public void setup(Context context) throws IOException
	{
		conf = context.getConfiguration();
	}
	
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
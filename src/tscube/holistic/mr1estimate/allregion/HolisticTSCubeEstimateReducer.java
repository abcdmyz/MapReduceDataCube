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

import datacube.common.StringPair;
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
	
		int count = 0;
		long sampleTupleSize = DataCubeParameter.getMRCubeTotalSampleSize(Long.valueOf(conf.get("total.tuple.size")));
		int regionNumber = DataCubeParameter.getLatticeRegionNumber(conf.get("dataset"));
		long totalTupleSize = sampleTupleSize *  regionNumber;
		
		int machineNumber = Integer.valueOf(conf.get("mapred.reduce.tasks"));
		if (machineNumber <= 1)
		{
			machineNumber = Integer.valueOf(conf.get("total.machine.number"));
		}
		
		int interval = (int) (totalTupleSize / (machineNumber - 1));
		int id = 1;

		System.out.println("boudary:" + totalTupleSize + " " + machineNumber + " " + interval);
	
		for (IntWritable val : values) 
	    {
			count++;
			
			if (count >= interval)
			{
				outputKey.set(key.getSecondString());
				outputValue.set(String.valueOf(id));
				
				context.write(outputKey, outputValue);
				
				count = 0;
				id++;
			}
	    }	
	}

	@Override
	public void cleanup(Context context) throws IOException
	{

	}
	

}
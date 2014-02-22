package mrcube.holistic.mr1estimate;

import java.io.IOException;
import java.net.URI;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;
import datacube.configuration.DataCubeParameter;

public class HolisticMRCubeEstimatePrintSampleReducer  extends Reducer<StringPair,IntWritable,Text,Text> 
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
		
		maxReducerLimitByte = Long.valueOf(conf.get("max.reducer.limit.byte"));
		percentMemUsage = Double.valueOf(conf.get("percent.mem.usage"));
		oneTupleSizeByByte = Integer.valueOf(conf.get("one.tuple.size.by.byte"));
		totalTupleSize = Long.valueOf(conf.get("total.tuple.size"));
		totalSampleSize = DataCubeParameter.getMRCubeTotalSampleSize(totalTupleSize);

		System.out.println("estimate: " + maxReducerLimitByte + " " + percentMemUsage + " " 
					+ oneTupleSizeByByte + " " + totalTupleSize + " " + totalSampleSize);
		
		heapMemAvail = (double)maxReducerLimitByte * (double)percentMemUsage;
		maxTupleByReducer = (double)heapMemAvail / (double)oneTupleSizeByByte;
		r = (double)maxTupleByReducer / (double)totalTupleSize;

		System.out.println(heapMemAvail + " " + maxTupleByReducer + " " + r);
	}
	
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		//calculationGroupBy(key, values, context);
		
		calculationGroupByPrint(key, values, context);
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
		
		outputKey.set("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		outputValue.set("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		
		context.write(outputKey, outputValue);
	}
	
	private void calculationGroupByPrint(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text region = new Text();
		Text maxNum = new Text();
	
		String last = null;
		int maxGroup = 0;
		int countGroup = 0;
		String maxGroupString = null;
		int totalTupleSize = 0;
	
		for (IntWritable val : values) 
	    {
			
			if (last != null && !key.getSecondString().equals(last))
			{
				region.set(key.getFirstString().toString() + " " + last + "|||");
				maxNum.set(String.valueOf(countGroup));
				context.write(region, maxNum);

				countGroup = 0;
			}

			last = key.getSecondString();
			countGroup++;
			
			totalTupleSize++;
	    }
		
		region.set(key.getFirstString().toString() + " " + last + "|||");
		maxNum.set(String.valueOf(countGroup));
		context.write(region, maxNum);
	}

	private void calculationGroupBy(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text region = new Text();
		Text maxNum = new Text();
	
		String last = null;
		int maxGroup = 0;
		int countGroup = 0;
		String maxGroupString = null;
		int totalTupleSize = 0;
	
		for (IntWritable val : values) 
	    {
			
			if (last != null && !key.getSecondString().equals(last))
			{
				if (countGroup > maxGroup)
				{
				   maxGroup = countGroup;
				   maxGroupString = last;
				}

				countGroup = 0;
			}

			last = key.getSecondString();
			countGroup++;
			
			totalTupleSize++;
	    }	
		
		if (countGroup > maxGroup)
		{
		   maxGroup = countGroup;
		   maxGroupString = last;
		}
		
		String regionPartitionFactor;
		
		int partitionFactor = 1;
		partitionFactor = determinePartitionFactor(maxGroup);	

		regionPartitionFactor = key.getFirstString();
		
		region.set(regionPartitionFactor);
		maxNum.set(String.valueOf(maxGroup) + " " + String.valueOf(partitionFactor));

		context.write(region, maxNum);
	}

	
	private int determinePartitionFactor(int maxGroup)
	{
		int partitionFactor = 1;
		
		//if (maxGroup >= 0.75 * r  * MRCubeParameter.getTotalSampleSize())
		{
			partitionFactor = (int) (maxGroup / (r * totalSampleSize));
		}
		
		if (partitionFactor <= 0)
		{
			partitionFactor = 1;
		}
		
		return partitionFactor;
	}
}
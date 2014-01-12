package mrcube.holistic.mr1estimate;

import java.io.IOException;
import java.net.URI;


import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.common.StringPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HolisticMRCubeEstimateReducer  extends Reducer<StringPair,IntWritable,Text,Text> 
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
	
	@Override
	public void setup(Context context) throws IOException
	{
		/*
		System.out.println("setup start");
		
		config = new Configuration();
		fs = FileSystem.get(config);

		path = new Path(MRCubeParameter.REGION_PARTITION_FILE_PATH);
		//fs.open(path);
		
		if (fs.exists(path))
		{
			fs.delete(path, true);
		}
	
		out = fs.create(path);

		System.out.println("setup end");
		*/
		
		heapMemAvail = (double)MRCubeParameter.MAX_REDUCER_LIMIT_BYTE * (double)MRCubeParameter.PERCENT_MEM_USAGE;
		maxTupleByReducer = (double)heapMemAvail / (double)MRCubeParameter.ONE_TUPLE_SIZE_BY_BYTE;
		r = (double)maxTupleByReducer / (double)MRCubeParameter.TOTAL_TUPLE_SIZE;
	}
	
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		calculationGroupBy(key, values, context);
		
		//justPrintKeyValue(key, values, context);
	}
	
	private void justPrintKeyValue(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		Text outputValue = new Text();
	
		for (IntWritable val : values) 
	    {
			outputKey.set(key.getFirstString().toString());
			outputValue.set(val.toString());
			
			context.write(outputKey, outputValue);
	    }
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
		
		String regionPartitionFactor;
		
		int partitionFactor = 1;
		partitionFactor = determinePartitionFactor(maxGroup);	

		regionPartitionFactor = key.getFirstString();
		
		region.set(regionPartitionFactor);
		maxNum.set(String.valueOf(maxGroup) + " " + String.valueOf(partitionFactor));

		context.write(region, maxNum);
	}

	@Override
	public void cleanup(Context context) throws IOException
	{
		/*
		System.out.println("cleanup start");
		
		fs.close();
		
		System.out.println("cleanup end");
		*/
	}
	
	private int determinePartitionFactor(int maxGroup)
	{
		int partitionFactor = 1;
		
		//if (maxGroup >= 0.75 * r  * MRCubeParameter.getTotalSampleSize())
		{
			partitionFactor = (int) (maxGroup / (r * MRCubeParameter.getTotalSampleSize()) + 1);
		}
		
		return partitionFactor;
	}
}
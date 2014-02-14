package WordCountStringPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import datacube.common.CubeLattice;
import datacube.common.StringPair;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;

public class WordCountStringPairMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private CubeLattice lattice = new CubeLattice(DataCubeParameter.getTestDataInfor().getAttributeSize(), DataCubeParameter.getTestDataInfor().getGroupAttributeSize());
     
	@Override
	public void setup(Context context)
	{

	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		produceWordCount(value, context);
		//justOutputTupleString(value, context);
		//justOutputValue(value, context);
	}

	void produceWordCount(Text value, Context context) throws IOException, InterruptedException
	{
		IntWritable outputValue = new IntWritable();
		StringPair regionGroupKey = new StringPair();
		
		String tupleSplit[] = value.toString().split("\\|");
		
		regionGroupKey.setFirstString(tupleSplit[0]);
		regionGroupKey.setSecondString(tupleSplit[1]);
		outputValue.set(Integer.valueOf(tupleSplit[2]));
		
		context.write(regionGroupKey, outputValue);
	}


}

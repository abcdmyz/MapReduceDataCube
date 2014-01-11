package mrcube.holistic.mr1estimate;

import java.io.IOException;
import java.util.Random;

import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.CubeLattice;
import mrcube.holistic.StringPair;
import mrcube.holistic.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HolisticMRCubeEstimateMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private CubeLattice lattice = new CubeLattice(MRCubeParameter.getTestDataInfor().getAttributeSize(), MRCubeParameter.getTestDataInfor().getGroupAttributeSize());
     
	@Override
	public void setup(Context context)
	{
		lattice.calculateAllRegion(MRCubeParameter.getTestDataInfor().getAttributeCubeRollUp());
		//lattice.printLattice();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		Random random = new Random();
		
		int randomNum = random.nextInt(MRCubeParameter.getSampleTuplePercent() * 10);
		//int randomNum = random.nextInt(10 * 10);
		
		/*
		if (randomNum <= 10)
		{
			produceAllRegionFromTule(value, context);
		}
		*/
		

		produceAllRegionFromTule(value, context);
		//justOutputTupleString(value, context);
		//justOutputValue(value, context);
	}

	void produceAllRegionFromTule(Text value, Context context) throws IOException, InterruptedException
	{
		Text groupValue = new Text();
		Text regionKey = new Text();
		Tuple<String> tuple;
		StringPair regionGroupKey = new StringPair();
		
		//tuple = MRCubeParameter.transformTestDataLineStringtoTuple(value.toString());
		String tupleSplit[] = value.toString().split("\t");
		Tuple<String> region = new Tuple<String>(MRCubeParameter.getTestDataInfor().getAttributeSize() + 1);
		
		for (int i = 0; i < lattice.getRegionBag().size(); i++)
		{
			String group = new String();
			
			for (int j = 0; j < lattice.getRegionBag().get(i).getSize(); j++)
			{
				if (lattice.getRegionBag().get(i).getField(j) != null)
				{
					//group += tuple.getField(j).toString() + " ";
					group += tupleSplit[j] + " ";
				}
			}
			
			regionGroupKey.setFirstString(lattice.getRegionStringBag().get(i));
			regionGroupKey.setSecondString(group);
			
			context.write(regionGroupKey, one);
		}
	}


	private void justOutputValue(Text value, Context context) throws IOException, InterruptedException
	{
		StringPair regionGroupKey = new StringPair();
		regionGroupKey.setFirstString(value.toString());
		regionGroupKey.setSecondString(one.toString());
		
		context.write(regionGroupKey, one);
	}
	
	private void justOutputTupleString(Text value, Context context) throws IOException, InterruptedException
	{
		Tuple<String> tuple;		
		tuple = MRCubeParameter.transformTestDataLineStringtoTuple(value.toString());
		
		StringPair regionGroupKey = new StringPair();
		regionGroupKey.setFirstString(tuple.toString('|'));
		regionGroupKey.setSecondString(one.toString());
		
		context.write(regionGroupKey, one);
	}
}
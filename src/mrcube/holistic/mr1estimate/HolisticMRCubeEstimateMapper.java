package mrcube.holistic.mr1estimate;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import datacube.common.datastructure.CubeLattice;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.Tuple;
import datacube.configuration.DataCubeParameter;

public class HolisticMRCubeEstimateMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private Configuration conf;
	private CubeLattice lattice;
	private IntWritable one = new IntWritable(1);	
     
	@Override
	public void setup(Context context)
	{
		conf = context.getConfiguration();
		lattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		lattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		
		//lattice.printLattice();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		Random random = new Random();
		int randomNum = random.nextInt(DataCubeParameter.getMRCubeSampleTuplePercent(Integer.valueOf(conf.get("total.tuple.size"))) * 10);
		
		if (randomNum <= 10)
		{
			produceAllRegionFromTuple(value, context);
		}

		//justOutputTupleString(value, context);
		//justOutputValue(value, context);
	}

	void produceAllRegionFromTuple(Text value, Context context) throws IOException, InterruptedException
	{
		Text groupValue = new Text();
		Text regionKey = new Text();
		Tuple<String> tuple;
		StringPair regionGroupKey = new StringPair();
		
		String tupleSplit[] = value.toString().split("\t");
		Tuple<String> region = new Tuple<String>(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize() + 1);
		
		for (int i = 0; i < lattice.getRegionBag().size(); i++)
		{
			String group = new String();
			
			for (int j = 0; j < lattice.getRegionBag().get(i).getSize(); j++)
			{
				if (lattice.getRegionBag().get(i).getField(j) != null)
				{
					if (group.length() > 0)
					{
						group += " " + tupleSplit[j];
					}
					else
					{
						group += tupleSplit[j];
					}
				}
			}
			
			regionGroupKey.setFirstString(lattice.getRegionStringSepLineBag().get(i));
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
		tuple = DataCubeParameter.transformTestDataLineStringtoTuple(value.toString(), conf.get("dataset"));
		
		StringPair regionGroupKey = new StringPair();
		regionGroupKey.setFirstString(tuple.toString('|'));
		regionGroupKey.setSecondString(one.toString());
		
		context.write(regionGroupKey, one);
	}
}
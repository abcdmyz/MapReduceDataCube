package groupby.terasort.algebraic.mr1estimate;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import datacube.common.datastructure.CubeLattice;
import datacube.configuration.DataCubeParameter;

public class TeraSortGroupByEstimateMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private CubeLattice lattice;
	private Configuration conf;
     
	@Override
	public void setup(Context context)
	{
		conf = context.getConfiguration();
		lattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		lattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		lattice.printLattice();
		
		//System.out.println(conf.get("total.tuple.size"));
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		Random random = new Random();
		int randomNum = random.nextInt(DataCubeParameter.getMRCubeSampleTuplePercent(Integer.valueOf(conf.get("total.tuple.size"))) * 10);
		
		if (randomNum <= 10)
		{
			produceAllRegionFromTule(value, context);
		}
		
		//justOutputTupleString(value, context);
		//justOutputValue(value, context);
	}

	void produceAllRegionFromTule(Text value, Context context) throws IOException, InterruptedException
	{
		Text regionGroupKey = new Text();
		String tupleSplit[] = value.toString().split("\t");

		int i = Integer.valueOf(conf.get("groupby.region.id"));
		
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
		
		regionGroupKey.set(group);
		context.write(regionGroupKey, one);
	}
}

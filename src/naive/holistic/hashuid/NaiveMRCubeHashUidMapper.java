package naive.holistic.hashuid;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import datacube.common.CubeLattice;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;

public class NaiveMRCubeHashUidMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private IntWritable uid = new IntWritable(1);
	private CubeLattice lattice = new CubeLattice(DataCubeParameter.getTestDataInfor().getAttributeSize(), DataCubeParameter.getTestDataInfor().getGroupAttributeSize());
     
	@Override
	public void setup(Context context)
	{
		lattice.calculateAllRegion(DataCubeParameter.getTestDataInfor().getAttributeCubeRollUp());
		//lattice.printLattice();
		Configuration conf = context.getConfiguration();
		System.out.println(conf.get("total.tuple.size"));
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		produceAllRegionFromTule(value, context);
		//justOutputTupleString(value, context);
		//justOutputValue(value, context);
	}

	void produceAllRegionFromTule(Text value, Context context) throws IOException, InterruptedException
	{
		Tuple<String> tuple;
		Text regionGroupKey = new Text();
		Text measureValue = new Text();
		
		String tupleSplit[] = value.toString().split("\t");
		Tuple<String> region = new Tuple<String>(DataCubeParameter.getTestDataInfor().getAttributeSize() + 1);
		
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
			
			regionGroupKey.set(i + "|" + group + "|");
			uid.set(Integer.valueOf(DataCubeParameter.getTestDataMeasureString(value.toString())));
			
			context.write(regionGroupKey, uid);
		}
	}


}

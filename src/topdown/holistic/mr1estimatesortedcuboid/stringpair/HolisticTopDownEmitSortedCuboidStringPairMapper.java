package topdown.holistic.mr1estimatesortedcuboid.stringpair;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import datacube.common.datastructure.BatchArea;
import datacube.common.datastructure.BatchAreaGenerator;
import datacube.common.datastructure.CubeLattice;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.Tuple;
import datacube.configuration.DataCubeParameter;

public class HolisticTopDownEmitSortedCuboidStringPairMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private CubeLattice cubeLattice;
	private ArrayList<Tuple<Integer>> regionTupleBag = new ArrayList<Tuple<Integer>>();
	private IntWritable one = new IntWritable(1);
	private ArrayList<BatchArea> batchAreaBag = new ArrayList<BatchArea>();
	private BatchAreaGenerator batchAreaGenerator = new BatchAreaGenerator();
	private Configuration conf;
	
	@Override
	public void setup(Context context) throws IOException
	{
		conf = context.getConfiguration();
		
		cubeLattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		cubeLattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		cubeLattice.printLattice();
			
		batchAreaBag = batchAreaGenerator.getBatchAreaPlan(conf.get("dataset"), cubeLattice);
		
		//printBatchArea();
		//System.out.println("batchArea Bag: " + batchAreaBag.size());
	} 	
     
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		int partitionFactor = 1;
		
		String tupleSplit[] = value.toString().split("\t");
		String pfKey = new String();
		String measureString = new String();

		IntWritable outputValue = new IntWritable();
		
		for (int i = 0; i < batchAreaBag.size(); i++)
		{
			partitionFactor = cubeLattice.getRegionBag().get(i).getPartitionFactor();
			pfKey = String.valueOf(DataCubeParameter.getTestDataPartitionFactorKey(value.toString(), partitionFactor, conf.get("dataset"), conf.get("datacube.measure")));
			measureString = DataCubeParameter.getTestDataMeasureString(value.toString(), conf.get("dataset"));

			String groupPublicKey = new String();
			String groupPipeKey = new String();
			String groupRegionID = new String();
			
			int terminal = batchAreaBag.get(i).getlongestRegionAttributeSize() - batchAreaBag.get(i).getallRegionIDSize() + 1;
			
			
			for (int k = 0; k < batchAreaBag.get(i).getlongestRegionAttributeSize(); k++)
			{
				int aid = batchAreaBag.get(i).getRegionAttribute(k);
				
				if (k >= terminal)
				{
					if (groupPipeKey.length() > 0)
					{
						groupPipeKey += " " + tupleSplit[aid];
					}
					else
					{
						groupPipeKey += tupleSplit[aid];
					}
				}
				else  //public
				{
					if (groupPublicKey.length() > 0)
					{
						groupPublicKey += " " + tupleSplit[aid];
					}
					else
					{
						groupPublicKey += tupleSplit[aid];
					}
				}
			}
			
			groupRegionID = String.valueOf(batchAreaBag.get(i).getallRegionID().get(0));
					
			StringPair outputKey = new StringPair();

			
			outputKey.setFirstString(groupRegionID + "|" +  groupPublicKey + "|");
			outputKey.setSecondString(groupPipeKey);
			
			if (conf.get("datacube.measure").equals("distinct"))
			{
				outputValue.set(Integer.valueOf(measureString));
			}
			else //count
			{
				outputValue.set(1);
			}
			
			context.write(outputKey, outputValue);
		}
	}
	
	private void printBatchArea()
	{
		for (int i = 0; i < batchAreaBag.size(); i++)
		{
			for (int k = 0; k < batchAreaBag.get(i).getLongestRegionAttributeID().size(); k++)
			{
				System.out.print(batchAreaBag.get(i).getLongestRegionAttributeID().get(k) + " ");
			}
			System.out.println();

			for (int k = 0; k < batchAreaBag.get(i).getallRegionIDSize(); k++)
			{
				System.out.print(batchAreaBag.get(i).getRegionID(k) + " ");
			}
			System.out.println();
		}
	}
}


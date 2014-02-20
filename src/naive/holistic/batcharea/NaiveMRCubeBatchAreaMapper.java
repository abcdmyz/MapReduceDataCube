package naive.holistic.batcharea;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import datacube.common.BatchArea;
import datacube.common.BatchAreaGenerator;
import datacube.common.CubeLattice;
import datacube.common.StringPair;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;

public class NaiveMRCubeBatchAreaMapper extends Mapper<Object, Text, StringPair, IntWritable> 
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
		String firstRegionID = new String();
		String pfKey = new String();
		String baSize = new String();
		String measureString = new String();
		String baType = new String();

		IntWritable outputValue = new IntWritable();
		
		for (int i = 0; i < batchAreaBag.size(); i++)
		{
			//firstRegionID = String.valueOf(batchAreaBag.get(i).getFirstRegionID());
			//baSize = String.valueOf(batchAreaBag.get(i).getBatchAreaSize());
			
			partitionFactor = cubeLattice.getRegionBag().get(i).getPartitionFactor();
			pfKey = String.valueOf(DataCubeParameter.getTestDataPartitionFactorKey(value.toString(), partitionFactor, conf.get("dataset")));
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
			
			for (int k = 0; k < batchAreaBag.get(i).getallRegionIDSize(); k++)
			{
				if (groupRegionID.length() > 0)
				{
					groupRegionID += " " + batchAreaBag.get(i).getallRegionID().get(k);
				}
				else
				{
					groupRegionID += batchAreaBag.get(i).getallRegionID().get(k);
				}
			}
			
			StringPair outputKey = new StringPair();

			
			outputKey.setFirstString(groupRegionID + "|" +  groupPublicKey + "|");
			outputKey.setSecondString(groupPipeKey);

			//System.out.println("key:" + outputKey.getFirstString() + " " + outputKey.getSecondString());
			
			outputValue.set(Integer.valueOf(measureString));
			
			context.write(outputKey, outputValue);
			
		}
	}
	
	private Tuple<Integer> StringLineToTuple(String line)
	{
		String[] regionSplit = line.split("\t"); 
		String[] pfSplit = regionSplit[1].split(" ");
		String[] groupSplit = regionSplit[0].split("\\|");
		
		Tuple<Integer> tuple = new Tuple<Integer>(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize());
		for (int i = 0; i < groupSplit.length; i++)
		{
			if (groupSplit[i].equals("*"))
			{
				tuple.addField(null);
			}
			else if (groupSplit[i].length() > 0)
			{
				tuple.addField(Integer.valueOf(groupSplit[i]));
			}
		}
		tuple.setPartitionFactor(Integer.valueOf(pfSplit[1]));
		
		return tuple;
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


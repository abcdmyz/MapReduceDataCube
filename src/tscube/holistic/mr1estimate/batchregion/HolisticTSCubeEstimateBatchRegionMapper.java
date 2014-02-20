package tscube.holistic.mr1estimate.batchregion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
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

public class HolisticTSCubeEstimateBatchRegionMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private CubeLattice lattice;
	private Configuration conf;
	private String oneString = "1";
	
	private ArrayList<Integer> batchSampleRegion = new ArrayList<Integer>();
	private BatchAreaGenerator batchAreaGenerator = new BatchAreaGenerator();
	private ArrayList<BatchArea> batchAreaBag = new ArrayList<BatchArea>();
     
	@Override
	public void setup(Context context)
	{
		conf = context.getConfiguration();
		lattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		lattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		
		batchSampleRegion = batchAreaGenerator.getTSCubeBatchSampleRegion(conf.get("dataset"));
		//lattice.printLattice();
		
		batchAreaBag = batchAreaGenerator.getBatchAreaPlan(conf.get("dataset"), lattice);
		
		for (int i = 0; i < batchAreaBag.size(); i++)
		{
			System.out.print("Batch Area:");
			for (int j = 0; j < batchAreaBag.get(i).getallRegionIDSize(); j++)
			{
				System.out.print(batchAreaBag.get(i).getRegionID(j) + " ");
			}
			System.out.println();
		}
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
		Text groupValue = new Text();
		Text regionKey = new Text();
		Tuple<String> tuple;
		StringPair outputKey = new StringPair();
		
		String tupleSplit[] = value.toString().split("\t");
		//Tuple<String> region = new Tuple<String>(DataCubeParameter.getTestDataInfor().getAttributeSize() + 1);
		
		for (int k = 0; k < batchSampleRegion.size(); k++)
		{
			String group = new String();
			String groupRegionID = new String();
			
			/*
			for (int i = 0; i < batchAreaBag.get(k).getallRegionIDSize(); i++)
			{
				if (groupRegionID.length() > 0)
				{
					groupRegionID += " " + batchAreaBag.get(k).getallRegionID().get(i);
				}
				else
				{
					groupRegionID += batchAreaBag.get(k).getallRegionID().get(i);
				}
			}
			*/
			
			int i = batchSampleRegion.get(k); 
			int batchStartRegionID = batchAreaBag.get(k).getRegionID(0);
					
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

			outputKey.setFirstString(oneString);
			outputKey.setSecondString(batchStartRegionID + "|" + group + "|" + DataCubeParameter.getTestDataMeasureString(value.toString(), conf.get("dataset")) + "|");
			
			context.write(outputKey, one);
		}
	}


	private void justOutputValue(Text value, Context context) throws IOException, InterruptedException
	{
		StringPair outputKey = new StringPair();
		
		outputKey.setFirstString(value.toString());
		outputKey.setSecondString(one.toString());
		
		context.write(outputKey, one);
	}
	
	private void justOutputTupleString(Text value, Context context) throws IOException, InterruptedException
	{
		Tuple<String> tuple;		
		tuple = DataCubeParameter.transformTestDataLineStringtoTuple(value.toString(), conf.get("dataset"));
		
		StringPair outputKey = new StringPair();
		
		outputKey.setFirstString(tuple.toString('|'));
		outputKey.setSecondString(one.toString());
		
		context.write(outputKey, one);
	}
}

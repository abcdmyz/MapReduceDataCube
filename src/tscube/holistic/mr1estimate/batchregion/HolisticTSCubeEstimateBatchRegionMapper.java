package tscube.holistic.mr1estimate.batchregion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

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

public class HolisticTSCubeEstimateBatchRegionMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private CubeLattice lattice;
	private Configuration conf;
	private String oneString = "1";

	private ArrayList<Integer> batchSampleRegion = new ArrayList<Integer>();
	private BatchAreaGenerator batchAreaGenerator = new BatchAreaGenerator();
	private ArrayList<BatchArea> batchAreaBag = new ArrayList<BatchArea>();
	private ArrayList<Integer> batchRootRegion = new ArrayList<Integer>();
      
	@Override
	public void setup(Context context)
	{
		conf = context.getConfiguration();
		lattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		lattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		//lattice.printLattice();
		
		batchSampleRegion = batchAreaGenerator.getTSCubeBatchSampleRegion(conf.get("dataset"));
		batchAreaBag = batchAreaGenerator.getBatchAreaPlan(conf.get("dataset"), lattice);
		batchRootRegion = batchAreaGenerator.getTSCubeBatchRootRegion(conf.get("dataset"));
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
		StringPair outputKey = new StringPair();
		int i;
		
		String tupleSplit[] = value.toString().split("\t");

		for (int k = 0; k < batchSampleRegion.size(); k++)
		{
			String group = new String();
			String groupRegionID = new String();

			if (conf.get("datacube.measure").equals("distinct"))
			{
				i = batchSampleRegion.get(k);
			}
			else //count
			{
				i = batchRootRegion.get(k);
			}
			
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
			
			if (conf.get("datacube.measure").equals("distinct"))
			{
				outputKey.setSecondString(batchStartRegionID + "|" + group + "|" + DataCubeParameter.getTestDataMeasureString(value.toString(), conf.get("dataset")) + "|");
			}
			else if (conf.get("datacube.measure").equals("count"))
			{
				outputKey.setSecondString(batchStartRegionID + "|" + group + "|" + DataCubeParameter.getTestDataTupleID(value.toString(), conf.get("dataset")) + "|");
			}
			else
			{
				//null
			}
			
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

package topdown.holistic.mr1emitsortedcuboid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class HolisticTopDownEmitSortedCuboidMapper extends Mapper<Object, Text, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);
	private String oneString = "1";
	private CubeLattice lattice;
	private Configuration conf;
	
	private ArrayList<Integer> batchRootRegion = new ArrayList<Integer>();
	private BatchAreaGenerator batchAreaGenerator = new BatchAreaGenerator();
	private ArrayList<BatchArea> batchAreaBag = new ArrayList<BatchArea>();
     
	@Override
	public void setup(Context context)
	{
		conf = context.getConfiguration();
		lattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		lattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		
		//lattice.printLattice();
		
		batchRootRegion = batchAreaGenerator.getTSCubeBatchSampleRegion(conf.get("dataset"));
		batchAreaBag = batchAreaGenerator.getBatchAreaPlan(conf.get("dataset"), lattice);
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		produceAllPipeRootRegionFromTule(value, context);
	}

	void produceAllPipeRootRegionFromTule(Text value, Context context) throws IOException, InterruptedException
	{
		Text groupValue = new Text();
		Text regionKey = new Text();
		Tuple<String> tuple;
		Text outputKey = new Text();
		
		String tupleSplit[] = value.toString().split("\t");
		
		for (int k = 0; k < batchRootRegion.size(); k++)
		{
			String group = new String();
			String groupRegionID = new String();
			
			
			int batchStartRegionID = batchAreaBag.get(k).getRegionID(0);
			int i = batchStartRegionID;
					
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

			if (conf.get("datacube.measure").equals("distinct"))
			{
				outputKey.set(batchStartRegionID + "|" + group + "|" + DataCubeParameter.getTestDataMeasureString(value.toString(), conf.get("dataset")) + "|");
				context.write(outputKey, one);
			}
			else if (conf.get("datacube.measure").equals("count"))
			{
				outputKey.set(batchStartRegionID + "|" + group + "|");
				context.write(outputKey, one);
			}
			else
			{
				//null
			}
		}
	}


	private void justOutputValue(Text value, Context context) throws IOException, InterruptedException
	{
		Text regionGroupKey = new Text();
		regionGroupKey.set(value.toString());
		
		context.write(regionGroupKey, one);
	}
	
	private void justOutputTupleString(Text value, Context context) throws IOException, InterruptedException
	{
		Tuple<String> tuple;		
		tuple = DataCubeParameter.transformTestDataLineStringtoTuple(value.toString(), conf.get("dataset"));
		
		Text regionGroupKey = new Text();
		regionGroupKey.set(tuple.toString('|'));
		
		context.write(regionGroupKey, one);
	}
}
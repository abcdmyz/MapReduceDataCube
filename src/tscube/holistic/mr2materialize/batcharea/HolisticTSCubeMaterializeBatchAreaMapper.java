package tscube.holistic.mr2materialize.batcharea;

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
import datacube.common.StringTripple;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;

public class HolisticTSCubeMaterializeBatchAreaMapper extends Mapper<Object, Text, StringTripple, IntWritable> 
{
	private CubeLattice cubeLattice;
	ArrayList<Tuple<Integer>> regionTupleBag = new ArrayList<Tuple<Integer>>();
	private IntWritable one = new IntWritable(1);
	private ArrayList<String> boundary;
	private ArrayList<BatchArea> batchAreaBag = new ArrayList<BatchArea>();
	private BatchAreaGenerator batchAreaGenerator = new BatchAreaGenerator();
	private Configuration conf;
	
	@Override
	public void setup(Context context) throws IOException
	{
		conf = context.getConfiguration();
		cubeLattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		cubeLattice.calculateAllRegion(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeCubeRollUp());
		
		boundary = new ArrayList<String>(Integer.valueOf(conf.get("total.machine.number")));

		String latticePath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("tscube.mr1.output.path") + conf.get("tscube.boundary.file.path");
		System.out.println("lattice Path: " + latticePath);
		
		Path path = new Path(latticePath);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
		
		try 
		{
			String line;
			line=br.readLine();
			while (line != null)
			{
				String[] regionSplit = line.split("\t"); 
				boundary.add(regionSplit[0]);
				
				line = br.readLine();
			}
			
			System.out.println("size: " + batchAreaBag.size());
			batchAreaBag = batchAreaGenerator.getBatchAreaPlan(conf.get("dataset"), cubeLattice);
			
			/*
			for (int i = 0; i < batchAreaBag.size(); i++)
			{
				System.out.print("Batch Area:");
				for (int j = 0; j < batchAreaBag.get(i).getallRegionIDSize(); j++)
				{
					System.out.print(batchAreaBag.get(i).getRegionID(j) + " ");
				}
				System.out.println();
			}
			*/
		} 	
		finally 
		{
			br.close();
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{

		String tupleSplit[] = value.toString().split("\t");
		String measureString = new String();
		int partitionerID;
		IntWritable outputValue = new IntWritable();
		
		for (int i = 0; i < batchAreaBag.size(); i++)
		{
			measureString = DataCubeParameter.getTestDataMeasureString(value.toString(), conf.get("dataset"));

			String groupPublicKey = new String();
			String groupPipeKey = new String();
			String groupRegionID = new String();
			String batchStartRegionID = new String();
			
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
			
			batchStartRegionID = String.valueOf(batchAreaBag.get(i).getRegionID(0));
			String boundaryCMPString = batchStartRegionID + "|" + groupPublicKey + "|" + measureString + "|"; 
			//System.out.println(boundaryCMPString);
			partitionerID = binarySearchPartitionerBoundary(boundaryCMPString);
			//Long.valueOf(measureString)
			
			StringTripple outputKey = new StringTripple();
			outputKey.setFirstString(groupRegionID + "|" + groupPublicKey + "|");
			outputKey.setSecondString(groupPipeKey);
			outputKey.setThirdString(String.valueOf(partitionerID));
			outputValue.set(Integer.valueOf(measureString));

			context.write(outputKey, outputValue);
		}
	}
	
	private int binarySearchPartitionerBoundary(String target)
	{
		int head = 0;
		int tail = boundary.size();
		
		if (target.compareTo(boundary.get(0)) <= 0)
		{
			return 0;
		}
		else if (target.compareTo(boundary.get(boundary.size()-1)) > 0)
		{
			return boundary.size();
		}
		
		while (head < tail - 1)
		{
			int mid = (head +  tail) / 2;
			
			if (target.equals(boundary.get(mid)))
			{
				return mid;
			}
			
			if (target.compareTo(boundary.get(mid)) > 0)
			{
				head = mid;
			}
			else
			{
				tail = mid;
			}
		}
		
		return tail;
	}
}



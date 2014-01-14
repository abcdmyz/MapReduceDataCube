package mrcube.holistic.mr2materialize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.common.CubeLattice;
import mrcube.holistic.common.StringMultiple;
import mrcube.holistic.common.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//StringMultiple
public class HolisticMRCubeMaterializeMapper extends Mapper<Object, Text, StringMultiple, IntWritable> 
{
	private CubeLattice cubeLattice;
	ArrayList<Tuple<Integer>> regionTupleBag = new ArrayList<Tuple<Integer>>();
	private IntWritable one = new IntWritable(1);
	
	@Override
	public void setup(Context context) throws IOException
	{
		cubeLattice = new CubeLattice(MRCubeParameter.testDataInfor.getAttributeSize(), MRCubeParameter.testDataInfor.getGroupAttributeSize());
		Configuration conf = context.getConfiguration();

		String latticePath = conf.get("hdfs.root.path") +  conf.get("dataset") + conf.get("mrcube.mr1.output.path") + conf.get("mrcube.region.partition.file.path");
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
				String[] pfSplit = regionSplit[1].split(" ");
				String[] groupSplit = regionSplit[0].split("\\|");
				
				Tuple<Integer> tuple = new Tuple<Integer>(MRCubeParameter.testDataInfor.getAttributeSize());
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
				regionTupleBag.add(tuple);
				
				line = br.readLine();
			}

			cubeLattice.setregionBag(regionTupleBag);
			cubeLattice.convertRegionBagToString();
		} 	
		finally 
		{
			br.close();
		}
	}
     
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		int partitionFactor = 1;
		
		String tupleSplit[] = value.toString().split("\t");
		String regionNum = new String();
		String pfKey = new String();
		String measureString = new String();
		
		for (int i = 0; i < cubeLattice.getRegionStringBag().size(); i++)
		{
			partitionFactor = cubeLattice.getRegionBag().get(i).getPartitionFactor();
			regionNum = String.valueOf(i);
			pfKey = String.valueOf(MRCubeParameter.getTestDataPartitionFactorKey(value.toString(), partitionFactor));
			measureString = MRCubeParameter.getTestDataMeasureString(value.toString());

			String groupKey = new String();
			
			for (int k = 0; k < cubeLattice.getRegionBag().get(i).getSize(); k++)
			{
				if (cubeLattice.getRegionBag().get(i).getField(k) != null)
				{
					if (groupKey.length() > 0)
					{
						groupKey += " " + tupleSplit[k];
					}
					else
					{
						groupKey += tupleSplit[k];
					}
				}
			}
			
			/*
			StringTripple outputKey = new StringTripple();

			outputKey.setFirstString(regionNum);
			outputKey.setSecondString(groupKey);
			outputKey.setThirdString(pfKey);
			*/
			
			StringMultiple outputKey = new StringMultiple(4);
			outputKey.addString(regionNum);
			outputKey.addString(groupKey);
			outputKey.addString(pfKey);
			outputKey.addString(measureString);

			/*
			Text outputKey = new Text();
			outputKey.set(regionNum + "|" + groupKey + "|" + pfKey + "|" + measureString + "|");
			*/
			
			context.write(outputKey, one);
		}
	}
}


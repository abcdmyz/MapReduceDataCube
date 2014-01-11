package mrcube.holistic.mr2materialize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.CubeLattice;
import mrcube.holistic.StringTripple;
import mrcube.holistic.Tuple;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HolisticMRCubeMaterializeMapper extends Mapper<Object, Text, StringTripple, IntWritable> 
{
	private CubeLattice cubeLattice;
	ArrayList<Tuple<Integer>> regionTupleBag = new ArrayList<Tuple<Integer>>();
	private IntWritable one = new IntWritable(1);
	
	@Override
	public void setup(Context context) throws IOException
	{
		cubeLattice = new CubeLattice(MRCubeParameter.testDataInfor.getAttributeSize(), MRCubeParameter.testDataInfor.getGroupAttributeSize());
		
		Path path = new Path(MRCubeParameter.REGION_PARTITION_FILE_PATH);
		
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
		
		for (int i = 0; i < cubeLattice.getRegionStringBag().size(); i++)
		{
			partitionFactor = cubeLattice.getRegionBag().get(i).getPartitionFactor();
			regionNum = String.valueOf(i);
			pfKey = String.valueOf(MRCubeParameter.getTestDataPartitionFactorKey(value.toString(), partitionFactor));

			String groupKey = new String();
			
			for (int k = 0; k < cubeLattice.getRegionBag().get(i).getSize(); k++)
			{
				if (cubeLattice.getRegionBag().get(i).getField(k) != null)
				{
					groupKey += tupleSplit[k] + " ";
				}
			}
			
			StringTripple outputKey = new StringTripple();

			outputKey.setFirstString(regionNum);
			outputKey.setSecondString(groupKey);
			outputKey.setThirdString(pfKey);
			
			/*
			Text outputKey = new Text();
			outputKey.set(regionNum + " " + groupKey + " " + pfKey);
			*/
			
			context.write(outputKey, one);
		}
	}
}


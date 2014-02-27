package mrcube.holistic.mr2materialize.stringpair;

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

import datacube.common.datastructure.CubeLattice;
import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.Tuple;
import datacube.configuration.DataCubeParameter;

//StringMultiple
public class HolisticMRCubeMaterializeStringPairMapper extends Mapper<Object, Text, StringPair, IntWritable> 
{
	private CubeLattice cubeLattice;
	ArrayList<Tuple<Integer>> regionTupleBag = new ArrayList<Tuple<Integer>>();
	private IntWritable one = new IntWritable(1);
	Configuration conf;
	
	@Override
	public void setup(Context context) throws IOException
	{
		conf = context.getConfiguration();
		cubeLattice = new CubeLattice(DataCubeParameter.getTestDataInfor(conf.get("dataset")).getAttributeSize(), DataCubeParameter.getTestDataInfor(conf.get("dataset")).getGroupAttributeSize());
		
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
				regionTupleBag.add(tuple);
				
				line = br.readLine();
			}

			cubeLattice.setregionBag(regionTupleBag);
			cubeLattice.sortRegionTupleBagReverse();
			cubeLattice.convertRegionBagToString('|');
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
		
		for (int i = 0; i < cubeLattice.getRegionStringSepLineBag().size(); i++)
		{
			partitionFactor = cubeLattice.getRegionBag().get(i).getPartitionFactor();
			regionNum = String.valueOf(i);
			pfKey = String.valueOf(DataCubeParameter.getTestDataPartitionFactorKey(value.toString(), partitionFactor, conf.get("dataset"), conf.get("datacube.measure")));
			measureString = DataCubeParameter.getTestDataMeasureString(value.toString(), conf.get("dataset"));

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
			
			StringPair outputKey = new StringPair();
			outputKey.setFirstString(regionNum + "|" + groupKey + "|" +  pfKey + "|");
			outputKey.setSecondString(measureString);

			context.write(outputKey, one);
		}
	}
}


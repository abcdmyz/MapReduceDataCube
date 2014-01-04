package mrcube.naive.text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NaiveMRCubeTextMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text measure = new Text();
	private Text groupby = new Text();
	//private static ArrayList[] cubeKey = new ArrayList[10];
     
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		String linesplit[] = line.split("\\|");
		String dimension[] = new String[4];
		String dimensionName[] = {"orderkey", "partkey", "suppkey", "shipdate"};
		int dimensionNumber = 4;
		
		
		dimension[0] = linesplit[0];
		dimension[1] = linesplit[1];
		dimension[2] = linesplit[2];
		dimension[3] = linesplit[10];
		
		measure.set(dimension[0] + " " + dimension[1] + " " + dimension[2] + " " + dimension[3] + " " 
				+ linesplit[4]);
		
		for (int i = 0; i < dimensionNumber; i++)
		{
			//groupby.set(dimensionName[i] + " " + dimension[i]);	
			groupby.set(dimension[i]);	
			context.write(groupby, measure);
		}
	
		for (int i = 0; i < dimensionNumber; i++)
		{
			for (int j = i+1; j < dimensionNumber; j++)
			{
				//groupby.set(dimensionName[i] + " " + dimensionName[j] + " " + dimension[i] + " " + dimension[j]);	
				groupby.set(dimension[i] + " " + dimension[j]);	
				context.write(groupby, measure);
			}
		}
		
		for (int i = 0; i < dimensionNumber; i++)
		{
			for (int j = i+1; j < dimensionNumber; j++)
			{
				for (int k = j+1; k < dimensionNumber; k++)
				//groupby.set(dimensionName[i] + " " + dimensionName[j] + " " + dimensionName[k] + " " + 
					//	dimension[i] + " " + dimension[j] + " " + dimension[k]);	
				groupby.set(dimension[i] + " " + dimension[j] + " " + dimension[k]);	
				context.write(groupby, measure);
			}
		}
	}
}

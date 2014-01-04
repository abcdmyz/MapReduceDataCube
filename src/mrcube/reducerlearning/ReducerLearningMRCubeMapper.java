package mrcube.reducerlearning;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReducerLearningMRCubeMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text measure = new Text();
	private Text groupby = new Text();
	//private static ArrayList[] cubeKey = new ArrayList[10];
     
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		String linesplit[] = line.split("\\|");
		String dimension[] = new String[4];
		//String dimensionName[] = {"orderkey", "partkey", "suppkey", "shipdate"};
		int dimensionNumber = 4;
		
		dimension[0] = linesplit[0];
		dimension[1] = linesplit[1];
		dimension[2] = linesplit[2];
		dimension[3] = linesplit[10];

		/*
		measure.set(linesplit[4]);
		//measure.set(dimension[0] + " " + dimension[1] + " " + dimension[2] + " " + dimension[3] + " " 
				//+ linesplit[4]);
		
		//measure.set(dimension[0] + " " + dimension[1] + " " + dimension[2] +  " " + linesplit[4]);
		groupby.set("02 " + dimension[0] + " " + dimension[2]);	
		context.write(groupby, measure);
		
		//measure.set(dimension[0] + " " + dimension[1] + " " + dimension[2] +  " " + linesplit[4]);
		groupby.set("13 " + dimension[1] + " " + dimension[3]);	
		context.write(groupby, measure);
		
		measure.set(dimension[0] + " " + dimension[1] + " " + dimension[2] +  " " + linesplit[4]);
		groupby.set("0 " + dimension[0]);	
		context.write(groupby, measure);
		
		measure.set(dimension[1] + " " + dimension[2] + " " + dimension[3] +  " " + linesplit[4]);
		groupby.set("1 " +dimension[1]);	
		context.write(groupby, measure);
		
		measure.set(dimension[2] + " " + dimension[3] + " " + dimension[1] +  " " + linesplit[4]);
		groupby.set("2 " + dimension[2]);	
		context.write(groupby, measure);
		*/
		
		measure.set(dimension[3] + " " + dimension[1] + " " + dimension[2] +  " " + linesplit[4]);
		groupby.set("3 " + dimension[3]);	
		context.write(groupby, measure);
	}
	
}


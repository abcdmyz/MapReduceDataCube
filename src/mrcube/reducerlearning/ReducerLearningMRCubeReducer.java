package mrcube.reducerlearning;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerLearningMRCubeReducer extends Reducer<Text,Text,Text,Text> 
{
	private Text resultValue = new Text();
	private Text resultKey = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		for (Text val : values) 
		{	
			//String valueString = val.toString();
			//String valueSplit[] = valueString.split(" ");
		
			//String length = String.valueOf(valueSplit.length);
			resultKey.set(key);
			resultValue.set("x");
			context.write(resultKey, resultValue);
		}
	}	
}
		
		/*
		String keyString = key.toString();
		String keySplit[] = keyString.split(" ");
		
		long sum0 = 0;
		long sum1 = 0;
		long sum2 = 0;
		
		String last1 = null;
		String last2 = null;
		
		//if (keySplit[0].equals("0") || keySplit[0].equals("1")  || keySplit[0].equals("2") || keySplit[0].equals("3"))
		{
			for (Text val : values) 
			{	
				String valueString = val.toString();
				String valueSplit[] = valueString.split(" ");
			
				//String measure = valueSplit[3];
				//long quantity = Integer.parseInt(measure);
				
				//resultKey.set("key= " + String.valueOf(valueSplit.length));
			   //resultValue.set("value= " + keySplit[0] + " " + valueString);
			   //context.write(resultKey, resultValue);
				
				//context.write(key, val);
				
				String length = String.valueOf(valueSplit.length);
				resultKey.set(valueString);
				resultValue.set("x");
				context.write(resultKey, resultValue);
				
				
				if (last1 == null && last2 == null)
				{
					sum0 = sum1 = sum2 = quantity;
					
					last1 = valueSplit[1];
					last2 = valueSplit[2];
					
					continue;
				}
				
				if (valueSplit[1] != last1)
				{
					resultKey.set(valueSplit[0] + " " + last1);
					resultValue.set(String.valueOf(sum1));
					context.write(resultKey, resultValue);
					
					resultKey.set(valueSplit[0] + " " + valueSplit[1] + " " + last2);
					resultValue.set(String.valueOf(sum2));	
					context.write(resultKey, resultValue);
					
					sum1 = sum2 = quantity;
				}
				else if (valueSplit[1] == last1 && valueSplit[2] != last2)
				{
					resultKey.set(valueSplit[0] + " " + valueSplit[1] + " " + last2);
					resultValue.set(String.valueOf(sum2));
					
					context.write(resultKey, resultValue);
					
					sum2 = quantity;
					sum1 += quantity;
				}
				else if (valueSplit[1] == last1 && valueSplit[2] == last2)
				{
					sum2 += quantity;
					sum1 += quantity;
				}
				
				sum0 += quantity;
				last1 = valueSplit[1];
				last2 = valueSplit[2];
				
			}
		}
		*/
		/*
		else
		{
			for (Text val : values) 
			{
				String valueString = val.toString();
				String valueSplit[] = valueString.split(" ");
			
				String measure = valueSplit[valueSplit.length-1];
				
				long quantity = Long.parseLong(measure);
				sum0 += quantity;
			}
		}
	
		String rk = new String();
		for (int i = 1; i < keySplit.length; i++)
		{
			rk += keySplit[i] + " ";
		}
		
		resultKey.set(rk);
	   resultValue.set(String.valueOf(sum0));
	   context.write(resultKey, resultValue);
	   */
	   



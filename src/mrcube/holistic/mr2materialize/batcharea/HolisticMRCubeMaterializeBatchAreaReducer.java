package mrcube.holistic.mr2materialize.batcharea;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.StringPair;

public class HolisticMRCubeMaterializeBatchAreaReducer extends Reducer<StringPair, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String keyFirstSplit[] = key.getFirstString().split("\\|");

		String attributeSplit[] = keyFirstSplit[0].split(" ");
		//System.out.println(key.getFirstString());
		//System.out.println(firstRegionID + " " + baSize);
		
		if (attributeSplit.length > 1)
		{
			//System.out.println("multi");
			multiRegionPipelineCalculation(key, values, context, attributeSplit, keyFirstSplit);
		}
		else
		{
			//System.out.println("one");
			oneRegionCalculation(key, values, context, attributeSplit, keyFirstSplit);
		}
		
		//justPrint(key, values, context);

	}
	
	private void justPrint(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();
		
		outputKey.set(key.getFirstString() + "*" + key.getSecondString());
		
		for(IntWritable val : values)
		{
			context.write(outputKey, val);
		}
	}
	
	private void oneRegionCalculation(StringPair key, Iterable<IntWritable> values, Context context, 
			String[] attributeSplit, String[] keyFirstSplit) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();

		HashSet<Integer> totalHash = new HashSet<Integer>();
		
		for(IntWritable val : values)
		{
			totalHash.add(val.get());
		}
		
		outputKey.set(keyFirstSplit[0] + "|" + keyFirstSplit[1] + "|");
		outputValue.set(totalHash.size());
		context.write(outputKey, outputValue);
		
	}
	
	private void multiRegionPipelineCalculation(StringPair key, Iterable<IntWritable> values, Context context, 
			String[] attributeSplit, String[] keyFirstSplit) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();
		int baSize =  attributeSplit.length;
		
		String pipePublic[] = new String[baSize];
		
		for (int i = 0; i < baSize; i++)
		{
			pipePublic[i] = attributeSplit[baSize - i - 1] + "|" + keyFirstSplit[1];
		}

		String curValue[] = new String[baSize];
		HashSet<Integer> curSet[] = new HashSet[baSize];
		HashSet<Integer> totalSet = new HashSet();
		
		for (int i = 0; i < baSize; i++)
		{
			curSet[i] = new HashSet<Integer>();
		}

		boolean start = false;
		boolean nextPrint = false;;
		String keySecondSplit[] = new String[baSize];
		
		for(IntWritable val : values)
		{
			keySecondSplit = key.getSecondString().split(" ");
			nextPrint = false;
			
			//System.out.println("key:" + key.getSecondString());
			//System.out.println("len:" + keySecondSplit.length);

			String pipePrivate = new String();
			
			for (int i = 0; i < keySecondSplit.length; i++)
			{
				//System.out.println(keySecondSplit[i] + " " + curValue[i] + " " + nextPrint);
				
				if (nextPrint || !keySecondSplit[i].equals(curValue[i]) || curValue[i] == null)
				{
					//System.out.println("start: " + start);
					if (start)
					{
						if (nextPrint)
						{
							pipePrivate += " " + curValue[i];
						}
						else
						{
							for (int k = 0; k <= i; k++)
							{
								pipePrivate += " " + curValue[k];
							}
						}

						outputKey.set(pipePublic[i+1] + pipePrivate + "|");	
						outputValue.set(curSet[i].size());

						//System.out.println("rout:" + pipePublic[i+1] + pipePrivate + "|\t" + curSet[i].size());

						context.write(outputKey, outputValue);
					}
					
					curValue[i] = keySecondSplit[i];
					curSet[i].clear();
					curSet[i].add(val.get());

					nextPrint = true;
				}
				else
				{
					//System.out.print("set2 " + keySecondSplit[i] + " " + val.get());
					curSet[i].add(val.get());
				}
			}
			
			start = true;
			totalSet.add(val.get());
		}

		String pipePrivate = new String();
		
		for (int i = 0; i < keySecondSplit.length; i++)
		{
			pipePrivate += " " + curValue[i];
			
			outputKey.set(pipePublic[i+1] + pipePrivate + "|");	
			outputValue.set(curSet[i].size());

			//System.out.println("rout:" + pipePublic[i+1] + pipePrivate + "|\t" + curSet[i].size());

			context.write(outputKey, outputValue);
		}
		
		outputKey.set(pipePublic[0] + "|");	
		outputValue.set(totalSet.size());
		context.write(outputKey, outputValue);
	}
}

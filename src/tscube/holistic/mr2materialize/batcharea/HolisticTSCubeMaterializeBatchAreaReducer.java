package tscube.holistic.mr2materialize.batcharea;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringPair;
import datacube.common.datastructure.StringTripple;

public class HolisticTSCubeMaterializeBatchAreaReducer extends Reducer<StringTripple, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		String keyFirstSplit[] = key.getFirstString().split("\\|");

		String attributeSplit[] = keyFirstSplit[0].split(" ");
		
		if (attributeSplit.length > 1)
		{
			multiRegionPipelineCalculation(key, values, context, attributeSplit, keyFirstSplit);
		}
		else
		{
			oneRegionCalculation(key, values, context, attributeSplit, keyFirstSplit);
		}
	}
	
	private void justPrint(StringTripple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();
		
		outputKey.set(key.getFirstString() + "*" + key.getSecondString() + "*" + key.getThirdString());
		
		for(IntWritable val : values)
		{
			context.write(outputKey, val);
		}
	}
	
	private void oneRegionCalculation(StringTripple key, Iterable<IntWritable> values, Context context, 
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
	
	private void multiRegionPipelineCalculation(StringTripple key, Iterable<IntWritable> values, Context context, 
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

			String pipePrivate = new String();
			
			for (int i = 0; i < keySecondSplit.length; i++)
			{	
				if (nextPrint || !keySecondSplit[i].equals(curValue[i]) || curValue[i] == null)
				{
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
						context.write(outputKey, outputValue);
					}
					
					curValue[i] = keySecondSplit[i];
					curSet[i].clear();
					curSet[i].add(val.get());

					nextPrint = true;
				}
				else
				{
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
			context.write(outputKey, outputValue);
		}
		
		outputKey.set(pipePublic[0] + "|");	
		outputValue.set(totalSet.size());
		context.write(outputKey, outputValue);
	}
}

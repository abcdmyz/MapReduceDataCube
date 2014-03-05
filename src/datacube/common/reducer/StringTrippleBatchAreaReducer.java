package datacube.common.reducer;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import datacube.common.datastructure.StringTriple;

public class StringTrippleBatchAreaReducer extends Reducer<StringTriple, IntWritable, Text, IntWritable> 
{
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(StringTriple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		
		String keyFirstSplit[] = key.getFirstString().split("\\|");
		String attributeSplit[] = keyFirstSplit[0].split(" ");
		
		if (conf.get("datacube.measure").equals("distinct"))
		{
			if (attributeSplit.length > 1)
			{
				multiRegionPipelineDistinctCalculation(key, values, context, attributeSplit, keyFirstSplit);
			}
			else
			{
				oneRegionDistinctCalculation(key, values, context, attributeSplit, keyFirstSplit);
			}
		}
		else if (conf.get("datacube.measure").equals("count"))
		{
			if (attributeSplit.length > 1)
			{
				multiRegionPipelineCountCalculation(key, values, context, attributeSplit, keyFirstSplit);
			}
			else
			{
				oneRegionCountCalculation(key, values, context, attributeSplit, keyFirstSplit);
			}
		}
		
		//justPrint(key, values, context);

	}
	
	private void justPrint(StringTriple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();
		
		outputKey.set(key.getFirstString() + "*" + key.getSecondString());
		
		for(IntWritable val : values)
		{
			context.write(outputKey, val);
		}
	}
	
	private void oneRegionDistinctCalculation(StringTriple key, Iterable<IntWritable> values, Context context, 
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
	
	private void multiRegionPipelineDistinctCalculation(StringTriple key, Iterable<IntWritable> values, Context context, 
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
	
	
	private void oneRegionCountCalculation(StringTriple key, Iterable<IntWritable> values, Context context, 
			String[] attributeSplit, String[] keyFirstSplit) throws IOException, InterruptedException
	{
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();

		int count = 0;
		
		for(IntWritable val : values)
		{
			count++;
		}
		
		outputKey.set(keyFirstSplit[0] + "|" + keyFirstSplit[1] + "|");
		outputValue.set(count);
		context.write(outputKey, outputValue);
		
	}
	
	private void multiRegionPipelineCountCalculation(StringTriple key, Iterable<IntWritable> values, Context context, 
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
		int curSet[] = new int[baSize];
		int totalSet = 0;
		
		for (int i = 0; i < baSize; i++)
		{
			curSet[i] = 0;
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
						outputValue.set(curSet[i]);
						context.write(outputKey, outputValue);
					}
					
					curValue[i] = keySecondSplit[i];
					curSet[i] = val.get();
					//curSet[i] = 1;

					nextPrint = true;
				}
				else
				{
					curSet[i] += val.get();
					//curSet[i]++;
				}
			}
			
			start = true;
			totalSet += val.get();
			//totalSet++;
		}

		String pipePrivate = new String();
		
		for (int i = 0; i < keySecondSplit.length; i++)
		{
			pipePrivate += " " + curValue[i];
			
			outputKey.set(pipePublic[i+1] + pipePrivate + "|");	
			outputValue.set(curSet[i]);

			context.write(outputKey, outputValue);
		}
		
		outputKey.set(pipePublic[0] + "|");	
		outputValue.set(totalSet);
		context.write(outputKey, outputValue);
	}
}

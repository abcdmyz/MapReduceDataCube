package datacube.common;

import java.util.ArrayList;

import datacube.configuration.DataCubeParameter;


public class BatchArea 
{
	private ArrayList<Tuple<Integer>> batch;
	private int[] regionParent;
	private int attributeSize;
	
	public BatchArea(int attributeSize)
	{
		this.attributeSize = attributeSize;
	}
	
	public void addRegion(Tuple<Integer> region, int rParent)
	{
		batch.add(region);
		regionParent[batch.size()-1] = rParent;
	}

	public Tuple<Integer> getRegion(int index)
	{
		return batch.get(index);
	}
 }

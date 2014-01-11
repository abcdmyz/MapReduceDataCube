package mrcube.holistic;

import java.util.ArrayList;

import mrcube.configuration.MRCubeParameter;

public class BatchArea 
{
	private ArrayList<Tuple<Integer>> batch;
	private int[] regionParent;
	private int attributeSize;
	
	public BatchArea(int attributeSize)
	{
		this.attributeSize = attributeSize;
		regionParent = new int[MRCubeParameter.MAX_BATCH_AREA_SIZE];
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

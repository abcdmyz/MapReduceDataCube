package datacube.common;

import java.util.ArrayList;

public class BatchAreaLattice 
{
	private ArrayList<BatchArea> batchAreaBag;
	private int batchAreaBagSize;

	public BatchAreaLattice(int batchAreaBagSize)
	{
		this.batchAreaBagSize = batchAreaBagSize;
		batchAreaBag = new ArrayList<BatchArea>(batchAreaBagSize);
	}
	
	public void addBatchArea(BatchArea batchArea)
	{
		batchAreaBag.add(batchArea);
	}
	
	public BatchArea getBatchArea(int index)
	{
		return batchAreaBag.get(index);
	}
	
}

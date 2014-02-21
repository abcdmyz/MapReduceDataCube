package datacube.common;

import java.util.ArrayList;

public class BatchAreaGenerator 
{
	private ArrayList<Integer> getOriginalBatchAreaPlan(String dataset)
	{
		ArrayList<Integer> order = new ArrayList(4);

		if (dataset.startsWith("d2"))
		{
			order.add(4);
			order.add(4);
			order.add(4);
			order.add(3);
		}
		if (dataset.startsWith("d3"))
		{
			order.add(2);
			order.add(2);
			order.add(2);
			order.add(1);
		}
		
		return order;
	}
	
	
	public ArrayList<Integer> getTSCubeBatchSampleRegion(String dataset)
	{
		ArrayList<Integer> order = new ArrayList(4);
		
		if (dataset.startsWith("d2"))
		{
			order.add(3);
			order.add(7);
			order.add(11);
			order.add(14);
		}
		if (dataset.startsWith("d3"))
		{
			order.add(1);
			order.add(3);
			order.add(5);
			order.add(6);
		}
		
		return order;
	}

	public ArrayList<BatchArea> getBatchAreaPlan(String dataset, CubeLattice cubeLattice)
	{
		ArrayList<BatchArea> batch = new ArrayList<BatchArea>();
		ArrayList<Integer> planOrder = getOriginalBatchAreaPlan(dataset);
		ArrayList<Integer> finalOrder = new ArrayList<Integer>();
		
		int cur = 0;
		int count = 1;
		int sum = 1;
		
		for (int i = 0; i < cubeLattice.getRegionBag().size(); i++)
		{
			
			if (sum > 1 && cubeLattice.getRegionBag().get(i).getPartitionFactor() != cubeLattice.getRegionBag().get(i-1).getPartitionFactor())
			{	
				finalOrder.add(count - 1);
				count = 1;
			}
			
			if (sum >= planOrder.get(cur))
			{
				if (count > 0)
				{
					finalOrder.add(count);
				}
				
				sum = 1;
				count = 1;
				cur++;
			}
			else
			{
				count++;
				sum++;
			}
		}

		System.out.print("final order:");
		for (int i = 0; i < finalOrder.size(); i++)
		{
			System.out.print(finalOrder.get(i) + " ");
		}
		System.out.println();
		
		int regionID = 0;
		
		for (int i = 0; i < finalOrder.size(); i++)
		{
			BatchArea area = new BatchArea();
			
			for (int k = 0; k < cubeLattice.getRegionBag().get(regionID).getSize(); k++)
			{
				if (cubeLattice.getRegionBag().get(regionID).getField(k) != null)
				{
					area.addRegionAttribute(k);
				}
			}
			for (int k = regionID; k < regionID + finalOrder.get(i); k++)
			{
				area.addRegionID(k);
			}
			
			batch.add(area);
			
			regionID += finalOrder.get(i);
		}

		return batch;
	}
}

package mrcube.holisticmeasure;

import java.util.ArrayList;

/*
 * Data cube lattice
 * Generate all region in the lattice, including hierarchical lattice
 */

public class Lattice 
{
	private ArrayList<Tuple<Integer>> regionBag;
	private int[] attributeParent;
	private int[] rollupGroup;
	private int attributeSize; 
	
	public Lattice(int attributeSize)
	{
		this.attributeSize = attributeSize;
		
		attributeParent = new int[attributeSize];
		rollupGroup = new int[attributeSize];
		
		regionBag = new ArrayList<Tuple<Integer>>();
	}
	
	public ArrayList<Tuple<Integer>> getRegionBag()
	{
		return regionBag;
	}
	
	/*
	 * Calculate all region according to the attribute information of cube and roll up
	 * 
	 * @param - record which attribute to cube and which attribute to roll up
	 * 			   for example, here are 8 attributes, numbered from 0-7.
	 * 				cube on (0,1,2), roll up (3,4,5), (6,7),
	 * 				The first tuple must record the cube on information, if there are no cube on, set the first tuple to null
	 * 				Other roll up information are record in the remaining tuples.
	 * 				so attributeCubeRollUp will contains 3 tuples, each record the above information
	 * 				see more details in /test/LatticeTest.java
	 * 				Pay attention, in roll up tuple, attribute must be in specific order, from parent to child.
	 * 				such as (year, month, day)
	 * 				All regions are in regionBag.
	 */
	public void calculateAllRegion(ArrayList<Tuple<Integer>> attributeCubeRollUp)
	{
		initializeBeforeCalculate(attributeCubeRollUp);

		Tuple<Integer> curRegion = new Tuple(attributeSize);
		boolean[] flag = new boolean[attributeSize];
		
		for (int i = 0; i < attributeSize; i++)
		{
			curRegion.addField(i, null);
		}
		
		dfsCalculateAllRegion(curRegion, flag, 0);
		
		/*
		 * print for debug
		 * 
		for (int i = 0; i < regionBag.size(); i++)
		{
			
			for (int j = 0; j < regionBag.get(i).getSize(); j++)
			{
				System.out.print(regionBag.get(i).getField(j) + " ");
			}
			
			System.out.println();
		}
		*/
	}
	
	/*
	 * Initialize attributeParent, rollupGroup before calculation
	 * atttriubteParent record the parent of the attribute.
	 * For cube on attribute, parent is itself.
	 * For roll up attribute, parent is itself if it is the root parent else the parent it the upper attribute
	 * For example, (year, month, day), year's parent is year, month's parent is year, day's parent is month
	 * rollupGroup record which roll up group the attribute belong to
	 * For cube on attribute, the group only contains itself.
	 * For roll up attribute, all attribute in the same hierarchy or roll uo belong to one roll up group
	 * For example, (year, month, day), attribute year, month, day all belong to year roll up group
	 * This 2 array is used for region calculation.
	 *   
	 * More example, here are 8 attributes, numbered from 0-7.
	 * cube on (0,1,2), roll up (3,4,5), (6,7),
	 * attributeParent={0,1,2,3,3,4,6,6}
	 * rollupGroup={0,1,2,3,3,3,6,6}
	 * 
	 * @param attributeCubeRollUp - record which attribute to cube and which attribute to roll up
	 * 				
	 */
	private void initializeBeforeCalculate(ArrayList<Tuple<Integer>> attributeCubeRollUp)
	{
		Tuple<Integer> tuple = attributeCubeRollUp.get(0);
		
		if (tuple != null)
		{
			for (int i = 0; i < tuple.getSize(); i++)
			{
				attributeParent[tuple.getField(i)] = tuple.getField(i);
				rollupGroup[tuple.getField(i)] = tuple.getField(i);
			}
		}
		
		for (int i = 1; i < attributeCubeRollUp.size(); i++)
		{
			tuple = attributeCubeRollUp.get(i);
			
			for (int j = 0; j < tuple.getSize(); j++)
			{
				if (j == 0)
				{
					attributeParent[tuple.getField(j)] = tuple.getField(j);
				}
				else
				{
					attributeParent[tuple.getField(j)] = tuple.getField(j-1);
				}
				
				rollupGroup[tuple.getField(j)] = tuple.getField(0);
			}
		}

		/*
		 * print for debug
		 * 
		for (int i = 0; i < attributeParent.length; i++)
		{
			System.out.print(attributeParent[i] + " ");
		}
		System.out.println();
		
		for (int i = 0; i < rollupGroup.length; i++)
		{
			System.out.print(rollupGroup[i] + " ");
		}
		System.out.println();
		*/
	}
	
	/*
	 * Use DFS to alculate all regions
	 * 
	 * @param curRegion - current region
	 * @param flag - mark the attribute that has been added
	 * @param start - the attribute current calculation start with
	 */
	private void dfsCalculateAllRegion(Tuple<Integer> curRegion, boolean[] flag, int start) 
	{
		if (start >= attributeSize)
		{
			return;
		}
		
		for (int i = start; i < attributeSize; i++)
		{
			/*
			 * In case duplicate choose
			 * for example, (year, month, day), if year is chosen, day won't be chosen.
			 */
			if (flag[rollupGroup[i]] == false)
			{
				curRegion.setField(i, i);
				flag[rollupGroup[i]] = true;
				
				/*
				 * handle roll up attribute
				 * for (year, month, day), if month is chosen, year must be chosen too.
				 */
				if (attributeParent[i] != i)
				{
					int k = i;
					
					while ( k != attributeParent[k] )
					{
						k = attributeParent[k];
						curRegion.setField(k, k);
					}
				}
				
				/*
				 * create a new object and add to regionBag
				 */
				Tuple<Integer> addRegion = new Tuple<Integer>(curRegion);
				regionBag.add(addRegion);
				
				dfsCalculateAllRegion(curRegion, flag, i + 1);
				
				/*
				 * back-track
				 */
				flag[rollupGroup[i]] = false;
				curRegion.setField(i, null);
				if (attributeParent[i] != i)
				{
					int k = i;
					
					while ( k != attributeParent[k] )
					{
						k = attributeParent[k];
						curRegion.setField(k, null);
					}
				}
			}
		}
	}
}

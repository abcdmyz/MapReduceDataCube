package datacube.test;

import java.util.ArrayList;

public class BatchAreaTest 
{
	public static void exec()
	{
		test1();
	}
	
	private static ArrayList<Integer> getOriginalBatchAreaPlan()
	{
		ArrayList<Integer> order = new ArrayList(4);

		order.add(4);
		order.add(4);
		order.add(4);
		order.add(3);

		return order;
	}

	public static void test1()
	{
		ArrayList<Integer> planOrder = getOriginalBatchAreaPlan();
		ArrayList<Integer> finalOrder = new ArrayList<Integer>();
		//int[] pf ={1, 1, 1, 4,  1, 1, 1, 2,  1, 1, 2, 1,  1, 1, 2};
		int[] pf = {1,1,1,1, 1,1,1,1, 1,1,1,2, 2,1,1};
		
		int cur = 0;
		int count = 1;
		int sum = 1;

		for (int i = 0; i < pf.length; i++)
		{
			/*
			if (pf[i] > 1)
			{
				if (count > 1)
				{
					finalOrder.add(count - 1);
					System.out.println("aa1 " +  (count -1));
				}
				
				finalOrder.add(1);
				System.out.println("one 1");
				
				count = 0;
			}
			*/
			
			if (sum > 1 && pf[i] != pf[i-1])
			{	
				finalOrder.add(count - 1);
				System.out.println("one 1");
				
				count = 1;
			}
			
			if (sum >= planOrder.get(cur))
			{
				if (count > 0)
				{
					finalOrder.add(count);
					System.out.println("bb1 " +  count);
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

		for (int i = 0; i < finalOrder.size(); i++)
		{
			System.out.print(finalOrder.get(i) + " ");
		}
		System.out.println();
	}
}

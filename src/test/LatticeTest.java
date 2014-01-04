package test;


import java.util.ArrayList;

import mrcube.holisticmeasure.Lattice;
import mrcube.holisticmeasure.Tuple;

public class LatticeTest 
{
	public static void exec()
	{
		test1();
		test2();
		test3();
	}
	
	/*
	 * has both cube on and roll up attributes
	 */
	public static void test1()
	{
		Lattice lattice = new Lattice(8);
		
		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
		
		Tuple<Integer> tuple0 = new Tuple<Integer>(3);
		tuple0.addField(0, 0);
		tuple0.addField(1, 1);
		tuple0.addField(2, 2);
	
		Tuple<Integer> tuple1 = new Tuple<Integer>(3);
		tuple1.addField(0, 3);
		tuple1.addField(1, 4);
		tuple1.addField(2, 5);
	
		Tuple<Integer> tuple2 = new Tuple<Integer>(2);
		tuple2.addField(0, 6);
		tuple2.addField(1, 7);
		
		aCubeRollUp.add(tuple0);
		aCubeRollUp.add(tuple1);
		aCubeRollUp.add(tuple2);
		
		lattice.calculateAllRegion(aCubeRollUp);
		ArrayList<Tuple<Integer>> regionBag = lattice.getRegionBag();
		
		for (int i = 0; i < regionBag.size(); i++)
		{
			
			for (int j = 0; j < regionBag.get(i).getSize(); j++)
			{
				System.out.print(regionBag.get(i).getField(j) + " ");
			}
			
			System.out.println();
		}	
	}
	
	/*
	 * has only cube on attribute
	 */
	public static void test2()
	{
		Lattice lattice = new Lattice(5);
		
		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
		
		Tuple<Integer> tuple0 = new Tuple<Integer>(3);
		tuple0.addField(0, 0);
		tuple0.addField(1, 1);
		tuple0.addField(2, 2);
	
		aCubeRollUp.add(tuple0);
		
		lattice.calculateAllRegion(aCubeRollUp);
		ArrayList<Tuple<Integer>> regionBag = lattice.getRegionBag();
		
		for (int i = 0; i < regionBag.size(); i++)
		{
			
			for (int j = 0; j < regionBag.get(i).getSize(); j++)
			{
				System.out.print(regionBag.get(i).getField(j) + " ");
			}
			
			System.out.println();
		}	
	}
	
	/*
	 * has only roll up attribute
	 */
	public static void test3()
	{
		Lattice lattice = new Lattice(5);
		
		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
	
		Tuple<Integer> tuple1 = new Tuple<Integer>(3);
		tuple1.addField(0, 0);
		tuple1.addField(1, 1);
		tuple1.addField(2, 2);
	
		Tuple<Integer> tuple2 = new Tuple<Integer>(2);
		tuple2.addField(0, 3);
		tuple2.addField(1, 4);
		
		aCubeRollUp.add(tuple1);
		aCubeRollUp.add(tuple2);
		
		lattice.calculateAllRegion(aCubeRollUp);
		ArrayList<Tuple<Integer>> regionBag = lattice.getRegionBag();
		
		for (int i = 0; i < regionBag.size(); i++)
		{
			
			for (int j = 0; j < regionBag.get(i).getSize(); j++)
			{
				System.out.print(regionBag.get(i).getField(j) + " ");
			}
			
			System.out.println();
		}	
	}
}

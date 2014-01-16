package mrcube.test;


import java.util.ArrayList;

import datacube.common.CubeLattice;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;


public class CubeLatticeTest 
{
	
	public static void exec()
	{
		test1();
		//test2();
		//test4();
	}
	
	/*
	 * has both cube on and roll up attributes
	 */
	public static void test1()
	{
		CubeLattice lattice = new CubeLattice(8,8);
		
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
		//printLattice(lattice);
		printLatticeStringSepLine(lattice);
		printLatticeStringSepBlank(lattice);
	}
	
	/*
	 * has only cube on attribute
	 */
	public static void test2()
	{
		CubeLattice lattice = new CubeLattice(5, 5);
		
		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
		
		Tuple<Integer> tuple0 = new Tuple<Integer>(3);
		tuple0.addField(0, 0);
		tuple0.addField(1, 1);
		tuple0.addField(2, 2);
	
		aCubeRollUp.add(tuple0);
		
		lattice.calculateAllRegion(aCubeRollUp);
		printLattice(lattice);
	}
	
	/*
	 * has only roll up attribute
	 */
	public static void test3()
	{
		CubeLattice lattice = new CubeLattice(6, 6);
		
		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
	
		Tuple<Integer> tuple1 = new Tuple<Integer>(3);
		tuple1.addField(0, 0);
		tuple1.addField(1, 1);
		tuple1.addField(2, 2);
	
		Tuple<Integer> tuple2 = new Tuple<Integer>(2);
		tuple2.addField(0, 3);
		tuple2.addField(1, 4);
		tuple2.addField(2, 5);

		Tuple<Integer> tuple3 = null;
		
		aCubeRollUp.add(tuple3);
		aCubeRollUp.add(tuple1);
		aCubeRollUp.add(tuple2);
		
		lattice.calculateAllRegion(aCubeRollUp);
	
		//printLattice(lattice);
		printLatticeStringSepLine(lattice);
	}
	
	public static void test4()
	{
		CubeLattice lattice = new CubeLattice(9, 6);
		
		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
		
		Tuple<Integer> tuple0 = null;

		Tuple<Integer> tuple1 = new Tuple<Integer>(3);
		tuple1.addField(0, 2);
		tuple1.addField(1, 3);
		tuple1.addField(2, 4);

		Tuple<Integer> tuple2 = new Tuple<Integer>(3);
		tuple2.addField(0, 5);
		tuple2.addField(1, 6);
		tuple2.addField(2, 7);
		
		aCubeRollUp.add(tuple0);
		aCubeRollUp.add(tuple1);
		aCubeRollUp.add(tuple2);
		
		lattice.calculateAllRegion(aCubeRollUp);
		
		printLattice(lattice);
		printLatticeStringSepLine(lattice);
		printLatticeStringSepBlank(lattice);
	}
	
	public static void printLattice(CubeLattice lattice)
	{
		ArrayList<Tuple<Integer>> regionBag = lattice.getRegionBag();
		int count = 0;
		
		for (int i = 0; i < regionBag.size(); i++)
		{
			
			for (int j = 0; j < regionBag.get(i).getSize(); j++)
			{
				if (regionBag.get(i).getField(j) == null)
					System.out.print("* ");
				
				else
					System.out.print(regionBag.get(i).getField(j) + " ");
			}
			
			count++;
			System.out.println();
		}
		
		System.out.println("Total Region: " + count);
	}

	public static void printLatticeStringSepLine(CubeLattice lattice)
	{
		for (int i = 0; i < lattice.getRegionStringSepLineBag().size(); i++)
		{
			System.out.println(lattice.getRegionStringSepLineBag().get(i));
		}
	}

	public static void printLatticeStringSepBlank(CubeLattice lattice)
	{
		for (int i = 0; i < lattice.getRegionStringSepBlankBag().size(); i++)
		{
			System.out.println(lattice.getRegionStringSepBlankBag().get(i));
		}
	}
}

package mrcube.configuration;

import java.util.ArrayList;

import mrcube.holistic.common.Tuple;

public class MRCubeTestData 
{	
	public static MRCubeTestDataInformation getTestData1()
	{
		MRCubeTestDataInformation dataInfor = new MRCubeTestDataInformation();

		ArrayList<Tuple<Integer>> aCubeRollUp = new ArrayList<Tuple<Integer>>();
		
		Tuple<Integer> tuple0 = new Tuple<Integer>(1);
		tuple0.addField(0, 0);

		Tuple<Integer> tuple1 = new Tuple<Integer>(2);
		tuple1.addField(0, 1);
		tuple1.addField(1, 2);

		aCubeRollUp.add(tuple0);
		aCubeRollUp.add(tuple1);

		dataInfor.setAttributeSize(3);
		dataInfor.setAttributeCubeRollUp(aCubeRollUp);
		
		return dataInfor;
	}


	public static MRCubeTestDataInformation getTestData2()
	{
		MRCubeTestDataInformation dataInfor = new MRCubeTestDataInformation();

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
	
		dataInfor.setAttributeSize(9);
		dataInfor.setGroupAttributeSize(6);
		dataInfor.setAttributeCubeRollUp(aCubeRollUp);
		
		return dataInfor;
	}

	public static Tuple<String> transformLineStringtoTupleForTestData1(String line)
	{
		Tuple<String> tuple = new Tuple(4);
		
		String lineSplit[] = line.split(" ");
		
		for (int i = 0; i < lineSplit.length; i++)
		{
			tuple.addField(lineSplit[i]);
		}
		
		return tuple;
	}

	public static Tuple<String> transformLineStringtoTupleForTestData2(String line)
	{
		Tuple<String> tuple = new Tuple(9);
		
		String lineSplit[] = line.split("\t");
		
		for (int i = 0; i < lineSplit.length; i++)
		{
			tuple.addField(lineSplit[i]);
		}
		
		return tuple;
	}

	public static int getPartitionFactorKeyForTestData2(String tuple, int partitionFactor)
	{
		String[] tupleSplit = tuple.split("\t");
		int uid = Integer.valueOf(tupleSplit[1]);
		
		return uid % partitionFactor;
	}

	public static String getMeasureStringForTestData2(String tuple)
	{
		String[] tupleSplit = tuple.split("\t");
		return tupleSplit[1];
	}
}

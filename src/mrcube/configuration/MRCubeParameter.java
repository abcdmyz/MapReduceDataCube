package mrcube.configuration;

import mrcube.holistic.common.Tuple;

public class MRCubeParameter 
{
	public static MRCubeTestDataInformation testDataInfor = MRCubeTestData.getTestData2();
	
	public static MRCubeTestDataInformation getTestDataInfor()
	{
		return MRCubeTestData.getTestData2();
	}

	public static int getTestDataPartitionFactorKey(String tuple, int partitionFactor)
	{
		return MRCubeTestData.getPartitionFactorKeyForTestData2(tuple, partitionFactor);
	}

	public static Tuple<String> transformTestDataLineStringtoTuple(String line)
	{
		return MRCubeTestData.transformLineStringtoTupleForTestData2(line);
	}	

	public static String getTestDataMeasureString(String tuple)
	{
		return MRCubeTestData.getMeasureStringForTestData2(tuple);
	}

	
	public static int getSampleTuplePercent(long totalTupleSize)
	{
		int samplePercent = 0;
		
		if (totalTupleSize >= 1000000)
		{
			samplePercent = 10000;
		}
		else if (totalTupleSize >= 1000 && totalTupleSize < 1000000)
		{
			samplePercent = 100;
		}
		else
		{
			samplePercent = 10;
		}

		return samplePercent;
	}

	public static int getTotalSampleSize(long totalTupleSize)
	{
		int sampleSize = (int) (totalTupleSize / getSampleTuplePercent(totalTupleSize));
		
		return sampleSize;
	}
}

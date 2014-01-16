package datacube.configuration;

import datacube.common.Tuple;

public class DataCubeParameter 
{
	public static DataCubeTestDataInformation testDataInfor = DataCubeTestData.getTestData2();
	
	public static DataCubeTestDataInformation getTestDataInfor()
	{
		return DataCubeTestData.getTestData2();
	}

	public static int getTestDataPartitionFactorKey(String tuple, int partitionFactor)
	{
		return DataCubeTestData.getPartitionFactorKeyForTestData2(tuple, partitionFactor);
	}

	public static Tuple<String> transformTestDataLineStringtoTuple(String line)
	{
		return DataCubeTestData.transformLineStringtoTupleForTestData2(line);
	}	

	public static String getTestDataMeasureString(String tuple)
	{
		return DataCubeTestData.getMeasureStringForTestData2(tuple);
	}

	
	public static int getMRCubeSampleTuplePercent(long totalTupleSize)
	{
		int samplePercent = 0;
		
		if (totalTupleSize > 1000000)
		{
			samplePercent = 10000;
		}
		else if (totalTupleSize >= 1000 && totalTupleSize <= 1000000)
		{
			samplePercent = 100;
		}
		else
		{
			samplePercent = 10;
		}

		return samplePercent;
	}

	public static int getMRCubeTotalSampleSize(long totalTupleSize)
	{
		int sampleSize = (int) (totalTupleSize / getMRCubeSampleTuplePercent(totalTupleSize));
		
		return sampleSize;
	}

	public static int getTSCubeSampleTuplePercent(long totalTupleSize)
	{
		int samplePercent = 0;
		
		if (totalTupleSize > 1000000)
		{
			samplePercent = 10000;
		}
		else if (totalTupleSize >= 1000 && totalTupleSize <= 1000000)
		{
			samplePercent = 100;
		}
		else
		{
			samplePercent = 10;
		}

		return samplePercent;
	}

	public static int getTSCubeTotalSampleSize(long totalTupleSize)
	{
		int sampleSize = (int) (totalTupleSize / getMRCubeSampleTuplePercent(totalTupleSize));
		
		return sampleSize;
	}
}

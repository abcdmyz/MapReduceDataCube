package datacube.configuration;

import datacube.common.Tuple;

public class DataCubeParameter 
{
	//public static DataCubeTestDataInformation testDataInfor = DataCubeTestData.getTestData2();
	
	public static DataCubeTestDataInformation getTestDataInfor(String dataset)
	{
		if (dataset.startsWith("d2"))
		{
			return DataCubeTestData.getTestData2();
		}
		else //d3 
		{
			return DataCubeTestData.getTestData3();
		}
	}

	public static int getTestDataPartitionFactorKey(String tuple, int partitionFactor, String dataset)
	{
		if (dataset.startsWith("d2"))
		{
			return DataCubeTestData.getPartitionFactorKeyForTestData2(tuple, partitionFactor);
		}
		else //d3
		{
			return DataCubeTestData.getPartitionFactorKeyForTestData3(tuple, partitionFactor);
		}
	}

	public static Tuple<String> transformTestDataLineStringtoTuple(String line, String dataset)
	{
		if (dataset.startsWith("d2"))
		{
			return DataCubeTestData.transformLineStringtoTupleForTestData2(line);
		}
		else //d3
		{
			return DataCubeTestData.transformLineStringtoTupleForTestData3(line);
		}
	}	

	public static String getTestDataMeasureString(String tuple, String dataset)
	{
		if (dataset.startsWith("d2"))
		{
			return DataCubeTestData.getMeasureStringForTestData2(tuple);
		}
		else //d3
		{
			return DataCubeTestData.getMeasureStringForTestData3(tuple);
		}
	}
	
	public static int getLatticeRegionNumber(String dataset)
	{
		if (dataset.startsWith("d2"))
		{
			return 15;
		}
		else //d3
		{
			return 7; 
		}
	}
	
	public static int getTSCubeBatchRegionNumber(String dataset)
	{
		if (dataset.startsWith("d2"))
		{
			return 4;
		}
		else //d3
		{
			return 4; 
		}
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

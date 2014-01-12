package mrcube.configuration;

import mrcube.holistic.common.Tuple;

public class MRCubeParameter 
{
	final public static int MAX_BATCH_AREA_SIZE = 10;
	final public static int MAX_REDUCER_LIMIT_BYTE = 1000 * 1000 * 1000;
	final public static float PERCENT_MEM_USAGE = 1;
	
	final public static long TOTAL_TUPLE_SIZE = 1000000000;
	final public static long ONE_TUPLE_SIZE_BY_BYTE = 50; 
	
	final public static String REGION_PARTITION_FILE_PATH = "/user/zhouyy/lattice_20/part-r-00000";
	final public static String MR1_INPUT_PATH = "input_d2_1000000";
	final public static String MR1_OUTPUT_PATH = "lattice";
	final public static String MR2_INPUT_PATH = "input_d2_20";
	final public static String MR2_OUTPUT_PATH = "output";
	final public static String MR3_INPUT_PATH = "output";
	final public static String MR3_OUTPUT_PATH = "output_end";
	final public static String MR3_INPUT_PATH_FILTER = ".*part.*";
	
	
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

	
	public static int getSampleTuplePercent()
	{
		int samplePercent = 0;
		
		/*
		if (TOTAL_TUPLE_SIZE > 2000000000) 
		{
			// for #rows beyond 2B, 2M samples are sufficient
			sampleSize = 2000000;
		} 
		else if (TOTAL_TUPLE_SIZE > 2000000 && TOTAL_TUPLE_SIZE < 2000000000) 
		{
			// for #rows between 2M to 2B, 100K tuples are sufficient
			sampleSize = 100000;
		}
		else 
		{
			sampleSize = 0;
        }
		*/
		/*
		if (MRCubeParameter.TOTAL_TUPLE_SIZE >= 1000000000)
		{
			samplePercent = 10000;
		}
		else
		{
			samplePercent = (int) (MRCubeParameter.TOTAL_TUPLE_SIZE / 100000);
		}
		*/
		samplePercent = 10000;
			
		
		return samplePercent;
	}

	public static int getTotalSampleSize()
	{
		return (int) (TOTAL_TUPLE_SIZE / getSampleTuplePercent());
	}
}

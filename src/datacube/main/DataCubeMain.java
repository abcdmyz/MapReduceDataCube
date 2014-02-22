package datacube.main;



import java.util.ArrayList;
import java.util.HashSet;

import naive.holistic.batcharea.NaiveMRCubeBatchArea;
import naive.holistic.stringpair.NaiveMRCubeStringPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import WordCountStringPair.WordCountStringPair;
import topdown.holistic.mr1emitsortedcuboid.HolisticTopDownEmitSortedCuboid;
import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimate;
import tscube.holistic.mr1estimate.batchregion.HolisticTSCubeEstimateBatchRegion;
import tscube.holistic.mr2materialize.batcharea.HolisticTSCubeMaterializeBatchArea;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterialize;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterializeNoCombiner;
import tscube.holistic.mr3postprocess.HolisticTSCubePostProcess;
import datacube.common.*;
import datacube.configuration.DataCubeParameter;
import datacube.test.*;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimate;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimatePrintSample;
import mrcube.holistic.mr2materialize.batcharea.HolisticMRCubeMaterializeBatchArea;
import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPair;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcess;


public class DataCubeMain extends Configured implements Tool
{
	private static HashSet dataCubeCMD = new HashSet<String>();
	private static HashSet dataSizeCMD = new HashSet<String>();
	private Configuration conf;
	
	private static void Initialize()
	{
		dataCubeCMD.add("naive");
		dataCubeCMD.add("naivehu");
		dataCubeCMD.add("mrcube");
		dataCubeCMD.add("mrcubemr1");
		dataCubeCMD.add("mrcubemr3");
		dataCubeCMD.add("mrcubemr1print");
		dataCubeCMD.add("tscubemr1");
		dataCubeCMD.add("tscubemr2");
		dataCubeCMD.add("tscubemr12");
		dataCubeCMD.add("tscubemr123");
		dataCubeCMD.add("tscube");
		dataCubeCMD.add("tscubeba");
		dataCubeCMD.add("tscubenc");
		dataCubeCMD.add("tscubemr23");
		dataCubeCMD.add("mrcubeba");
		dataCubeCMD.add("naiveba");
		dataCubeCMD.add("wordcount");
		dataCubeCMD.add("tscubebamr1");
		
		
		dataSizeCMD.add("100");
		dataSizeCMD.add("1000");
		dataSizeCMD.add("1000000");
		dataSizeCMD.add("10000000");
		dataSizeCMD.add("100000000");
		dataSizeCMD.add("1000000000");
		dataSizeCMD.add("10");
	}
	
	
	public static void main(String[] args) throws Exception 
	{
		Initialize();
		int res = ToolRunner.run(new DataCubeMain(), args);
		
		//CubeLatticeTest.exec();
		//BinarySearchPartitionerBoundaryTest.exec();
		//BatchAreaTest.exec();
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration.addDefaultResource("datacube-configuration.xml");
		conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		long startTime;
		long endTime;

		/*
		if (!dataCubeCMD.contains(otherArgs[0]))
		{
			System.err.println(otherArgs[0] + " Wrong Data Cube CMD\n" +
					"Usage:<data cube:mrcube/naive/tera> <data size:100/1000/1000000/10000000/100000000/1000000000>");
			System.exit(2);
		}
		if (!dataSizeCMD.contains(conf.get("total.tuple.size")))
		{
			System.err.println(conf.get("total.tuple.size") + " Wrong Data Size\n" +
					"Usage:<data cube:mrcube/naive/tera> <data size:100/1000/1000000/10000000/100000000/1000000000>");
			System.exit(2);
		}
		*/
		
		
		if (otherArgs[0].equals("naive"))
		{
			NaiveMRCubeStringPair mrCube = new NaiveMRCubeStringPair();
			
			startTime = System.currentTimeMillis();
			mrCube.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_naive_sp_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("mrcube"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();
			HolisticMRCubeMaterializeStringPair mrCube2 = new HolisticMRCubeMaterializeStringPair();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();
			
			startTime = System.currentTimeMillis();
			mrCube1.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr1_sp_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));

			startTime = System.currentTimeMillis();
			mrCube2.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr2_sp_" + conf.get("total.tuple.size") + "  Time: " + ((endTime-startTime)/1000));
			
			
			startTime = System.currentTimeMillis();
			mrCube3.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr3_sp_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("mrcubemr1"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();

			mrCube1.run(conf);	
		}
		else if (otherArgs[0].equals("mrcubemr3"))
		{
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();
			
			mrCube3.run(conf);
		}
		else if (otherArgs[0].equals("tscubemr1"))
		{
			HolisticTSCubeEstimate tsCube1 = new HolisticTSCubeEstimate();
			tsCube1.run(conf);
		}
		else if (otherArgs[0].equals("tscubebamr1"))
		{
			HolisticTSCubeEstimateBatchRegion tsCube1 = new HolisticTSCubeEstimateBatchRegion();
			tsCube1.run(conf);
		}
		else if (otherArgs[0].equals("tscubemr2"))
		{
			HolisticTSCubeMaterialize tsCube2 = new HolisticTSCubeMaterialize();
			
			tsCube2.run(conf);
		}
		else if (otherArgs[0].equals("tscubemr12"))
		{
			HolisticTSCubeEstimate tsCube1 = new HolisticTSCubeEstimate();
			HolisticTSCubeMaterialize tsCube2 = new HolisticTSCubeMaterialize();
			
			tsCube1.run(conf);
			tsCube2.run(conf);
		}
		else if (otherArgs[0].equals("tscube") || otherArgs[0].equals("tscubemr123"))
		{
			HolisticTSCubeEstimate tsCube1 = new HolisticTSCubeEstimate();
			HolisticTSCubeMaterialize tsCube2 = new HolisticTSCubeMaterialize();
			HolisticTSCubePostProcess tsCube3 = new HolisticTSCubePostProcess();
			
			tsCube1.run(conf);
			tsCube2.run(conf);
			tsCube3.run(conf);
		}
		else if (otherArgs[0].equals("tscubeba"))
		{
			HolisticTSCubeEstimateBatchRegion tsCube1 = new HolisticTSCubeEstimateBatchRegion();
			HolisticTSCubeMaterializeBatchArea tsCube2 = new HolisticTSCubeMaterializeBatchArea();
			HolisticTSCubePostProcess tsCube3 = new HolisticTSCubePostProcess();
			
			tsCube1.run(conf);
			tsCube2.run(conf);
			tsCube3.run(conf);
		}
		else if (otherArgs[0].equals("tscubenc"))
		{
			HolisticTSCubeEstimate tsCube1 = new HolisticTSCubeEstimate();
			HolisticTSCubeMaterializeNoCombiner tsCube2 = new HolisticTSCubeMaterializeNoCombiner();
			HolisticTSCubePostProcess tsCube3 = new HolisticTSCubePostProcess();
			
			tsCube1.run(conf);
			tsCube2.run(conf);
			tsCube3.run(conf);
		}
		else if (otherArgs[0].equals("tscubemr23"))
		{
			HolisticTSCubeMaterialize tsCube2 = new HolisticTSCubeMaterialize();
			HolisticTSCubePostProcess tsCube3 = new HolisticTSCubePostProcess();
			
			tsCube2.run(conf);
			tsCube3.run(conf);
		}
		else if (otherArgs[0].equals("mrcubeba"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();
			HolisticMRCubeMaterializeBatchArea mrcube2 = new HolisticMRCubeMaterializeBatchArea();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();
			
			mrCube1.run(conf);
			mrcube2.run(conf);
			mrCube3.run(conf);
		}
		else if (otherArgs[0].equals("naiveba"))
		{
			NaiveMRCubeBatchArea mrCube1 = new NaiveMRCubeBatchArea();
			
			mrCube1.run(conf);
		}
		else if (otherArgs[0].equals("wordcount"))
		{
			WordCountStringPair mrCube1 = new WordCountStringPair();
			mrCube1.run(conf);
		}
		else if (otherArgs[0].equals("mrcubemr1print"))
		{
			HolisticMRCubeEstimatePrintSample mrCube1Print = new HolisticMRCubeEstimatePrintSample();
			mrCube1Print.run(conf);
		}
		else if (otherArgs[0].equals("topdcubemr1"))
		{
			HolisticTopDownEmitSortedCuboid mr1 = new HolisticTopDownEmitSortedCuboid();
			mr1.run(conf);
		}
		
		
		return 0;
	}
}

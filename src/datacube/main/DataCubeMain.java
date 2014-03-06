package datacube.main;



import groupby.base.algebraic.BaseGroupBy;
import groupby.terasort.algebraic.mr1estimate.TeraSortGroupByEstimate;
import groupby.terasort.algebraic.mr2materialize.stringpair.TeraSortGroupByMaterializeStringPair;
import groupby.terasort.algebraic.mr3postprocess.TeraSortGroupByPostProcess;

import java.util.ArrayList;
import java.util.HashSet;

import naive.algebraic.text.NaiveMRCubeText;
import naive.holistic.batcharea.NaiveMRCubeBatchArea;
import naive.holistic.stringpair.NaiveMRCubeStringPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidText;
import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidTextNoCombiner;
import topdown.holistic.mr1emitsortedcuboid.text.HolisticTopDownEmitSortedCuboidTextNoReducer;
import topdown.holistic.mr1estimatesortedcuboid.stringpair.HolisticTopDownEmitSortedCuboidStringPair;
import topdown.holistic.mr2pipeline.HolisticTopDownPipeline;
import tscube.holistic.mr1estimate.allregion.HolisticTSCubeEstimate;
import tscube.holistic.mr1estimate.batchregion.HolisticTSCubeEstimateBatchRegion;
import tscube.holistic.mr2materialize.batcharea.HolisticTSCubeMaterializeBatchArea;
import tscube.holistic.mr2materialize.batcharea.HolisticTSCubeMaterializeBatchAreaNoCombiner;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterialize;
import tscube.holistic.mr2materialize.stringtripple.HolisticTSCubeMaterializeNoCombiner;
import tscube.holistic.mr3postprocess.HolisticTSCubePostProcess;
import datacube.common.datastructure.*;
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
	
	public static void main(String[] args) throws Exception 
	{
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
		
		if (otherArgs[0].equals("naive"))
		{
			NaiveMRCubeStringPair mrCube = new NaiveMRCubeStringPair();
			
			startTime = System.currentTimeMillis();
			mrCube.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_naive_sp_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("naiveba"))
		{
			NaiveMRCubeBatchArea mrCube1 = new NaiveMRCubeBatchArea();
			
			mrCube1.run(conf);
		}
		else if (otherArgs[0].equals("naivetext"))
		{
			NaiveMRCubeText mrCube1 = new NaiveMRCubeText();
			
			mrCube1.run(conf);
		}
		else if (otherArgs[0].equals("mrcube"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();
			HolisticMRCubeMaterializeStringPair mrCube2 = new HolisticMRCubeMaterializeStringPair();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();

			mrCube1.run(conf);
			mrCube2.run(conf);
			mrCube3.run(conf);
		}
		else if (otherArgs[0].equals("mrcubemr1"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();

			mrCube1.run(conf);	
		}
		else if (otherArgs[0].equals("mrcubemr1print"))
		{
			HolisticMRCubeEstimatePrintSample mrCube1Print = new HolisticMRCubeEstimatePrintSample();
			mrCube1Print.run(conf);
		}
		else if (otherArgs[0].equals("mrcubemr2"))
		{
			HolisticMRCubeMaterializeStringPair mrCube2 = new HolisticMRCubeMaterializeStringPair();
			mrCube2.run(conf);
		}
		else if (otherArgs[0].equals("mrcubemr3"))
		{
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();
			
			mrCube3.run(conf);
		}
		else if (otherArgs[0].equals("mrcubebamr2"))
		{
			HolisticMRCubeMaterializeBatchArea mrCube2 = new HolisticMRCubeMaterializeBatchArea();
			mrCube2.run(conf);
		}
		else if (otherArgs[0].equals("mrcubemr23"))
		{
			HolisticMRCubeMaterializeStringPair mrCube2 = new HolisticMRCubeMaterializeStringPair();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();

			mrCube2.run(conf);
			mrCube3.run(conf);
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
		else if (otherArgs[0].equals("mrcubebamr23"))
		{
			HolisticMRCubeMaterializeBatchArea mrcube2 = new HolisticMRCubeMaterializeBatchArea();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();

			mrcube2.run(conf);
			mrCube3.run(conf);
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
		else if (otherArgs[0].equals("tscubebamr23"))
		{
			HolisticTSCubeMaterializeBatchArea tsCube2 = new HolisticTSCubeMaterializeBatchArea();
			HolisticTSCubePostProcess tsCube3 = new HolisticTSCubePostProcess();
			
			tsCube2.run(conf);
			tsCube3.run(conf);
		}
		else if (otherArgs[0].equals("tscubebanc"))
		{
			HolisticTSCubeEstimateBatchRegion tsCube1 = new HolisticTSCubeEstimateBatchRegion();
			HolisticTSCubeMaterializeBatchAreaNoCombiner tsCube2 = new HolisticTSCubeMaterializeBatchAreaNoCombiner();
			HolisticTSCubePostProcess tsCube3 = new HolisticTSCubePostProcess();
			
			tsCube1.run(conf);
			tsCube2.run(conf);
			tsCube3.run(conf);
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
		else if (otherArgs[0].equals("topdcubemr1"))
		{
			HolisticTopDownEmitSortedCuboidText mr1 = new HolisticTopDownEmitSortedCuboidText();
			mr1.run(conf);
		}
		else if (otherArgs[0].equals("topdcubencmr1"))
		{
			HolisticTopDownEmitSortedCuboidTextNoCombiner mr1 = new HolisticTopDownEmitSortedCuboidTextNoCombiner();
			mr1.run(conf);
		}
		else if (otherArgs[0].equals("topdcubemr2"))
		{
			HolisticTopDownPipeline mr2 = new HolisticTopDownPipeline();
	
			mr2.run(conf);
		}
		else if (otherArgs[0].equals("topdcube"))
		{
			HolisticTopDownEmitSortedCuboidText mr1 = new HolisticTopDownEmitSortedCuboidText();
			HolisticTopDownPipeline mr2 = new HolisticTopDownPipeline();
			
			mr1.run(conf);
			mr2.run(conf);
		}
		else if (otherArgs[0].equals("topdcubenc"))
		{
			HolisticTopDownEmitSortedCuboidTextNoCombiner mr1 = new HolisticTopDownEmitSortedCuboidTextNoCombiner();
			HolisticTopDownPipeline mr2 = new HolisticTopDownPipeline();
			
			mr1.run(conf);
			mr2.run(conf);
		}
		else if (otherArgs[0].equals("topdcubenr"))
		{
			HolisticTopDownEmitSortedCuboidTextNoReducer mr1 = new HolisticTopDownEmitSortedCuboidTextNoReducer();
			HolisticTopDownPipeline mr2 = new HolisticTopDownPipeline();
			
			mr1.run(conf);
			mr2.run(conf);
		}
		else if (otherArgs[0].equals("topdcubesp"))
		{
			HolisticTopDownEmitSortedCuboidStringPair mr1 = new HolisticTopDownEmitSortedCuboidStringPair();
			HolisticTopDownPipeline mr2 = new HolisticTopDownPipeline();
			
			mr1.run(conf);
			mr2.run(conf);
		}
		else if (otherArgs[0].equals("topdcubespmr1"))
		{
			HolisticTopDownEmitSortedCuboidStringPair mr1 = new HolisticTopDownEmitSortedCuboidStringPair();
			mr1.run(conf);
		}
		else if (otherArgs[0].equals("gbbase"))
		{
			BaseGroupBy mr1 = new BaseGroupBy();
			mr1.run(conf);
		}
		else if (otherArgs[0].equals("gbtsmr1"))
		{
			TeraSortGroupByEstimate mr1 = new TeraSortGroupByEstimate();
			mr1.run(conf);
		}
		else if (otherArgs[0].equals("gbtsmr12"))
		{
			TeraSortGroupByEstimate mr1 = new TeraSortGroupByEstimate();
			TeraSortGroupByMaterializeStringPair mr2 = new TeraSortGroupByMaterializeStringPair();
			mr1.run(conf);
			mr2.run(conf);
		}
		else if (otherArgs[0].equals("gbts"))
		{
			TeraSortGroupByEstimate mr1 = new TeraSortGroupByEstimate();
			TeraSortGroupByMaterializeStringPair mr2 = new TeraSortGroupByMaterializeStringPair();
			TeraSortGroupByPostProcess mr3 = new TeraSortGroupByPostProcess();
			
			mr1.run(conf);
			mr2.run(conf);
			mr3.run(conf);
		}
		else
		{
			System.out.println("Wrong CMD");
		}
		
		
		return 0;
	}
}

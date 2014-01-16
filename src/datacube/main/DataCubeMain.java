package datacube.main;



import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import datacube.common.CubeLattice;
import datacube.common.Tuple;
import datacube.configuration.DataCubeParameter;

import mrcube.holistic.mr1estimate.HolisticMRCubeEstimate;
import mrcube.holistic.mr2materialize.stringmultiple.HolisticMRCubeMaterializeStringMultiple;
import mrcube.holistic.mr2materialize.stringpair.HolisticMRCubeMaterializeStringPair;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcess;
import mrcube.naive.stringpair.NaiveMRCubeStringPair;
import mrcube.naive.text.NaiveMRCubeText;
import mrcube.test.CubeLatticeTest;


public class DataCubeMain extends Configured implements Tool
{
	private static HashSet dataCubeCMD = new HashSet<String>();
	private static HashSet dataSizeCMD = new HashSet<String>();
	private Configuration conf;
	
	private static void Initialize()
	{
		dataCubeCMD.add("naive");
		dataCubeCMD.add("naivetext");
		dataCubeCMD.add("naivesp");
		dataCubeCMD.add("mrcubesm");
		dataCubeCMD.add("mrcubesp");
		dataCubeCMD.add("tera");
		dataCubeCMD.add("sample");
		
		dataSizeCMD.add("100");
		dataSizeCMD.add("1000");
		dataSizeCMD.add("1000000");
		dataSizeCMD.add("10000000");
		dataSizeCMD.add("100000000");
		dataSizeCMD.add("1000000000");
	}
	
	
	public static void main(String[] args) throws Exception 
	{
		Initialize();
		int res = ToolRunner.run(new DataCubeMain(), args);
		
		//CubeLatticeTest.exec();
	}


	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration.addDefaultResource("datacube-configuration.xml");
		conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		long startTime;
		long endTime;
		
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
		
		if (otherArgs[0].equals("naive"))
		{
			NaiveMRCubeStringPair mrCube = new NaiveMRCubeStringPair();
			
			startTime = System.currentTimeMillis();
			mrCube.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_naive_sp_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("naivesp"))
		{
			NaiveMRCubeStringPair mrCube = new NaiveMRCubeStringPair();
			
			startTime = System.currentTimeMillis();
			mrCube.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_naive_sp_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("naivetext"))
		{
			NaiveMRCubeText mrCube = new NaiveMRCubeText();
			
			startTime = System.currentTimeMillis();
			mrCube.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_naive_text_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("mrcubesm"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();
			HolisticMRCubeMaterializeStringMultiple mrCube2 = new HolisticMRCubeMaterializeStringMultiple();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();
			
			startTime = System.currentTimeMillis();
			mrCube1.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr1_sm_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));

			startTime = System.currentTimeMillis();
			mrCube2.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr2_sm_" + conf.get("total.tuple.size") + "  Time: " + ((endTime-startTime)/1000));
			
			
			startTime = System.currentTimeMillis();
			mrCube3.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr3_sm_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("mrcubesp"))
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
		else if (otherArgs[0].equals("sample"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();
			
			startTime = System.currentTimeMillis();
			mrCube1.run(conf);
			endTime = System.currentTimeMillis(); 
			System.out.println("mrcube_mr1_sample_" + conf.get("total.tuple.size") + " Time: " + ((endTime-startTime)/1000));
		}
		else if (otherArgs[0].equals("tera"))
		{
			System.out.println("Not Yet Implement");
		}
		
		return 0;
	}


}

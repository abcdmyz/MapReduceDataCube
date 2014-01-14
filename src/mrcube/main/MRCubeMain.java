package mrcube.main;



import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.CubeLatticeTest;
import mrcube.configuration.MRCubeParameter;
import mrcube.holistic.common.CubeLattice;
import mrcube.holistic.common.Tuple;
import mrcube.holistic.mr1estimate.HolisticMRCubeEstimate;
import mrcube.holistic.mr2materialize.HolisticMRCubeMaterialize;
import mrcube.holistic.mr3postprocess.HolisticMRCubePostProcess;
import mrcube.naive.NaiveMRCube;


public class MRCubeMain extends Configured implements Tool
{
	private static HashSet dataCubeCMD = new HashSet<String>();
	private static HashSet dataSizeCMD = new HashSet<String>();
	private Configuration conf;
	
	private static void Initialize()
	{
		dataCubeCMD.add("naive");
		dataCubeCMD.add("mrcube");
		dataCubeCMD.add("tera");
		
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
		int res = ToolRunner.run(new MRCubeMain(), args);
	}


	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration.addDefaultResource("datacube-configuration.xml");
		conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
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
			NaiveMRCube mrCube = new NaiveMRCube();
			mrCube.run(conf);
		}
		
		else if (otherArgs[0].equals("mrcube"))
		{
			HolisticMRCubeEstimate mrCube1 = new HolisticMRCubeEstimate();
			HolisticMRCubeMaterialize mrCube2 = new HolisticMRCubeMaterialize();
			HolisticMRCubePostProcess mrCube3 = new HolisticMRCubePostProcess();
		
			mrCube1.run(conf);
			mrCube2.run(conf);
			mrCube3.run(conf);
		}
		else if (otherArgs[0].equals("tera"))
		{
			System.out.println("Not Yet Implement");
		}
		
		return 0;
	}


}

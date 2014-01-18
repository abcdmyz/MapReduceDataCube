package mrcube.holistic.mr3postprocess;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import datacube.configuration.DataCubeParameter;

public class HolisticMRCubePostProcessFilePathFilter extends Configured implements PathFilter 
{
	Pattern pattern;
	Configuration conf;
	FileSystem fs;

	@Override
	public boolean accept(Path path) 
	{
		//System.out.println(path);

		try {
			if(fs.isDirectory(path))
			{
				return true;
			}
			else
			{
				Matcher m = pattern.matcher(path.toString());
				return m.matches();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void setConf(Configuration conf) 
	{
		this.conf = conf;
		if (conf != null)
		{
			try {
				fs = FileSystem.get(conf);
				String inputPathFilter = conf.get("mrcube.mr3.input.path.filter");
				pattern = Pattern.compile(inputPathFilter);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}


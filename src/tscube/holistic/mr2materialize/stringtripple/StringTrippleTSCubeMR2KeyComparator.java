package tscube.holistic.mr2materialize.stringtripple;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringPair;
import datacube.common.StringTripple;

public class StringTrippleTSCubeMR2KeyComparator extends WritableComparator 
{
	protected StringTrippleTSCubeMR2KeyComparator()
	{
		super(StringTripple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringTripple p1 = (StringTripple)w1;
		StringTripple p2 = (StringTripple)w2;
		
		 if (!p1.getFirstString().equals(p2.getFirstString()))
		{
			return p1.getFirstString().compareTo(p2.getFirstString());
		}
		else
		{
			return p1.getSecondString().compareTo(p2.getSecondString());
		}
	}
}

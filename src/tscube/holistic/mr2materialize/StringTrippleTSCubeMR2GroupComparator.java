package tscube.holistic.mr2materialize;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringPair;
import datacube.common.StringTripple;

public class StringTrippleTSCubeMR2GroupComparator extends WritableComparator 
{

	protected StringTrippleTSCubeMR2GroupComparator() 
	{
		super(StringTripple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringTripple p1 = (StringTripple)w1;
		StringTripple p2 = (StringTripple)w2;
		
		/*
		if (!p1.getThirdString().equals(p2.getThirdString()))
		{
			p1.getThirdString().compareTo(p2.getThirdString());
		}
		*/
				
		return p1.getFirstString().compareTo(p2.getFirstString());
	}
}

package mrcube.holistic.mr2materialize.stringmultiple;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringMultiple;

public class StringMultipleMRCubeMR2GroupComparator extends WritableComparator 
{

	protected StringMultipleMRCubeMR2GroupComparator() 
	{
		super(StringMultiple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringMultiple t1 = (StringMultiple)w1;
		StringMultiple t2 = (StringMultiple)w2;

		/*
		if (t1.getFirstString().equals(t2.getFirstString()))
		{
			return t1.getSecondString().compareTo(t2.getSecondString());
		}
		
		return t1.getFirstString().compareTo(t2.getFirstString());
		*/
		
		return t1.compareTo(t2, 3);
	}
}

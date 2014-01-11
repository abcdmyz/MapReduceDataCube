package mrcube.holistic.mr1estimate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mrcube.holistic.StringPair;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringPairGroupComparator extends WritableComparator 
{

	protected StringPairGroupComparator() 
	{
		super(StringPair.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringPair p1 = (StringPair)w1;
		StringPair p2 = (StringPair)w2;
		
		String sp1 = p1.getFirstString();
		String sp2 = p2.getFirstString();
		
		return sp1.compareTo(sp2);
	}
}

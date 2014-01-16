package tscube.holistic.mr1estimate;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringMultiple;

public class StringMultipleTSCubeMR1GroupComparator extends WritableComparator 
{

	protected StringMultipleTSCubeMR1GroupComparator() 
	{
		super(StringMultiple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringMultiple t1 = (StringMultiple)w1;
		StringMultiple t2 = (StringMultiple)w2;

		return t1.getString(0).compareTo(t2.getString(0));
	}
}

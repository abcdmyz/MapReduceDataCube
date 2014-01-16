package tscube.holistic.mr1estimate;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringMultiple;

public class StringMultipleTSCubeMR1KeyComparator extends WritableComparator 
{
	protected StringMultipleTSCubeMR1KeyComparator()
	{
		super(StringMultiple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringMultiple t1 = (StringMultiple)w1;
		StringMultiple t2 = (StringMultiple)w2;
	
		int cmp = t1.compareTo(t2, 3);
		
		if (cmp == 0)
		{
			Integer uid1 = Integer.valueOf(t1.getString(3));
			Integer uid2 = Integer.valueOf(t2.getString(3));
			
			return uid1.compareTo(uid2);
		}
		
		return cmp;
	}
}

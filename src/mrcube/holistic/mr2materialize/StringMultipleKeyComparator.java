package mrcube.holistic.mr2materialize;

import mrcube.holistic.common.StringMultiple;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringMultipleKeyComparator extends WritableComparator 
{
	protected StringMultipleKeyComparator()
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

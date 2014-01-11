package mrcube.holistic.mr2materialize;

import java.util.prefs.InvalidPreferencesFormatException;

import mrcube.holistic.StringPair;
import mrcube.holistic.StringTripple;
import mrcube.holistic.Tuple;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringTrippleKeyComparator extends WritableComparator 
{
	protected StringTrippleKeyComparator()
	{
		super(StringTripple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringTripple t1 = (StringTripple)w1;
		StringTripple t2 = (StringTripple)w2;
	
		return t1.compareTo(t2);
	}
}

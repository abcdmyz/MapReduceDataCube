package mrcube.holistic.mr2materialize;

import java.util.prefs.InvalidPreferencesFormatException;

import mrcube.holistic.StringPair;
import mrcube.holistic.StringTripple;
import mrcube.holistic.Tuple;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringTrippleGroupComparator extends WritableComparator 
{

	protected StringTrippleGroupComparator() 
	{
		super(StringTripple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringTripple t1 = (StringTripple)w1;
		StringTripple t2 = (StringTripple)w2;

		if (t1.getFirstString().equals(t2.getFirstString()))
		{
			return t1.getSecondString().compareTo(t2.getSecondString());
		}
		
		return t1.getFirstString().compareTo(t2.getFirstString());
	}

}

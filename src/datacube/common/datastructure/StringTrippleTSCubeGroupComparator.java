package datacube.common.datastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringTrippleTSCubeGroupComparator extends WritableComparator 
{

	protected StringTrippleTSCubeGroupComparator() 
	{
		super(StringTripple.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringTripple p1 = (StringTripple)w1;
		StringTripple p2 = (StringTripple)w2;
				
		return p1.getFirstString().compareTo(p2.getFirstString());
	}
}

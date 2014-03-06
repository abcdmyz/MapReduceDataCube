package datacube.common.datastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringTripleTSCubeGroupComparator extends WritableComparator 
{

	protected StringTripleTSCubeGroupComparator() 
	{
		super(StringTriple.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringTriple p1 = (StringTriple)w1;
		StringTriple p2 = (StringTriple)w2;
			
		if (!p1.getThirdString().equals(p2.getThirdString()))
		{
			return p1.getFirstString().compareTo(p2.getFirstString());
		}
		return p1.getThirdString().compareTo(p2.getThirdString());
	}
}

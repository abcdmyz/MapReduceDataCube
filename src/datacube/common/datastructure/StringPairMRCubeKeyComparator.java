package datacube.common.datastructure;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringPairMRCubeKeyComparator extends WritableComparator 
{
	protected StringPairMRCubeKeyComparator()
	{
		super(StringPair.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringPair p1 = (StringPair)w1;
		StringPair p2 = (StringPair)w2;
		
		return p1.compareTo(p2);
	}
}

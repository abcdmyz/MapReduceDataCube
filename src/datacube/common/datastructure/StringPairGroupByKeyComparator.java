package datacube.common.datastructure;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringPairGroupByKeyComparator extends WritableComparator 
{
	protected StringPairGroupByKeyComparator()
	{
		super(StringPair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		StringPair p1 = (StringPair)w1;
		StringPair p2 = (StringPair)w2;
		
		String sp1 = p1.getFirstString();
		String sp2 = p2.getFirstString();
		
		if (!p1.getSecondString().equals(p2.getSecondString()))
		{
			return sp1.compareTo(sp2);
		}
		else
		{
			return p1.getFirstString().compareTo(p2.getFirstString());
		}
	}
}
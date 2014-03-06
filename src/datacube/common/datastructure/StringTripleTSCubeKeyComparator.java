package datacube.common.datastructure;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringTripleTSCubeKeyComparator extends WritableComparator 
{
	protected StringTripleTSCubeKeyComparator()
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
			return p1.getThirdString().compareTo(p2.getThirdString());
		}
		else if (!p1.getFirstString().equals(p2.getFirstString()))
		{
			return p1.getFirstString().compareTo(p2.getFirstString());
		}
		else
		{
			return p1.getSecondString().compareTo(p2.getSecondString());
		}
	}
}

package naive.holistic.text;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringPair;

public class TextMRCubeNaiveKeyComparator extends WritableComparator 
{
	protected TextMRCubeNaiveKeyComparator()
	{
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		Text p1 = (Text)w1;
		Text p2 = (Text)w2;
		
		String split1[] = p1.toString().split("\\|");
		String split2[] = p2.toString().split("\\|");
		
		String p11 = split1[0] + "|" + split1[1] + "|";
		String p12 = split1[2];
		String p21 = split2[0] + "|" + split2[1] + "|";
		String p22 = split2[2];
	
		if (p11.equals(p21))
		{
			return p12.compareTo(p22);
		}
		
		return p11.compareTo(p21);
		
		/*
		if (!split1[0].equals(split2[0]))
		{
			return split1[0].compareTo(split2[0]);
		}
		else if (!split1[1].equals(split2[1]))
		{
			return split1[1].compareTo(split2[1]);
		}
		else
		{
			return split1[2].compareTo(split2[2]);
		}
		*/
	}
}

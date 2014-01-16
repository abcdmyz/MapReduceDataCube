package mrcube.naive.text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import datacube.common.StringPair;

public class TextMRCubeNaiveGroupComparator extends WritableComparator 
{

	protected TextMRCubeNaiveGroupComparator() 
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
		String p21 = split2[0] + "|" + split2[1] + "|";
	
		return p11.compareTo(p21);
	}
}

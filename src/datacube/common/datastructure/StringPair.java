package datacube.common.datastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable<StringPair>
{
	private String first;
	private String second;

	public StringPair()
	{
		first = new String();
		second = new String();
	}
	
	public StringPair(String first, String second)
	{
		this.first = first;
		this.second = second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		// TODO Auto-generated method stub
		first = in.readUTF();
		second = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		// TODO Auto-generated method stub
		out.writeUTF(first);
		out.writeUTF(second);
	}

	@Override
	public int compareTo(StringPair o) 
	{
		// TODO Auto-generated method stub
		if (!first.equals(o.first))
		{
			return first.compareTo(o.first);
		}
		else if (!second.equals(o.second))
		{
			return second.compareTo(o.second);
		}
		else
		{
			return 0;
		}
	}

	public String getFirstString()
	{
		return first;
	}
	
	public String getSecondString()
	{
		return second;
	}
	
	public void setFirstString(String s)
	{
		first = s;
	}
	
	public void setSecondString(String s)
	{
		second = s;
	}
}

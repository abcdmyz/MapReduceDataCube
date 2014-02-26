package datacube.common.datastructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringTripple implements WritableComparable<StringTripple>
{
	private String first;
	private String second;
	private String third;

	public StringTripple()
	{
		first = new String();
		second = new String();
		third = new String();
	}
	
	public StringTripple(String first, String second, String third)
	{
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		// TODO Auto-generated method stub
		first = in.readUTF();
		second = in.readUTF();
		third = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		// TODO Auto-generated method stub
		out.writeUTF(first);
		out.writeUTF(second);
		out.writeUTF(third);
	}

	@Override
	public int compareTo(StringTripple o) 
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
			return third.compareTo(o.third);
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
	
	public String getThirdString()
	{
		return third;
	}
	
	public void setFirstString(String s)
	{
		first = s;
	}
	
	public void setSecondString(String s)
	{
		second = s;
	}

	public void setThirdString(String s)
	{
		third = s;
	}
}

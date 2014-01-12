package mrcube.holistic.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class StringMultiple implements WritableComparable<StringMultiple>
{
	private int size;
	private ArrayList<String> stringList;
	
	public StringMultiple()
	{
		
	}
	
	public StringMultiple(int size)
	{
		stringList = new ArrayList<String>(size);
	}

	public void addString(String s)
	{
		stringList.add(s);
	}
	
	public String getString(int index)
	{
		return stringList.get(index);
	}

	public void setString(int index, String s)
	{
		stringList.set(index, s);
	}

	public int getSize()
	{
		return stringList.size();
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		size = in.readInt();
		stringList = new ArrayList<String>(size);
		
		for (int i = 0; i < size; i++)
		{
			stringList.add(in.readUTF());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeInt(stringList.size());
		for (int i = 0; i <  stringList.size(); i++)
		{
			out.writeUTF(stringList.get(i));
		}
	}

	@Override
	public int compareTo(StringMultiple other) 
	{
		int i = 0;
		
		while (i <  stringList.size() && other.getString(i).equals(stringList.get(i)))
		{
			i++;
		}
		
		if (i >= stringList.size())
		{
			return 0;
		}
		
		return stringList.get(i).compareTo(other.getString(i));
	}
	
	public int compareTo(StringMultiple other, int n) 
	{
		int i = 0;
		
		while (i <  n && other.getString(i).equals(stringList.get(i)))
		{
			i++;
		}
		
		if (i >= n)
		{
			return 0;
		}
		
		return stringList.get(i).compareTo(other.getString(i));
	}
}

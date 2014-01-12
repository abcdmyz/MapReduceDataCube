package mrcube.holistic.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

/*
 * encapsulation of ArrayList
 * Tuple is used for multi purpose, as region, group, ...
 */
public class Tuple<T>
{
	private ArrayList<T> fieldList;
	private int size;
	private int partitionFactor;
	
	public Tuple(int size)
	{
		this.size = size;
		fieldList = new ArrayList<T>(size);
		partitionFactor = 1;
	}

	public Tuple(Tuple t)
	{
		size = t.size;
		fieldList = (ArrayList<T>) t.fieldList.clone();
	}
	
	public int getPartitionFactor() 
	{
		return partitionFactor;
	}

	public void setPartitionFactor(int partitionFactor) 
	{
		this.partitionFactor = partitionFactor;
	}
	
	public T getField(int index)
	{
		return fieldList.get(index);
	}
	
	public void setFieldList(ArrayList<T> fieldList)
	{
		this.fieldList = (ArrayList<T>) fieldList.clone();
	}
	
	public void setField(int index, T element)
	{
		fieldList.set(index, element);
	}

	public void addField(int index, T element)
	{
		fieldList.add(index, element);
	}
	
	public int getSize()
	{
		return fieldList.size();
	}
	
	public void remove(int index)
	{
		fieldList.remove(index);
	}
	
	public void addField(T element)
	{
		fieldList.add(element);
	}

	@Override
	public String toString()
	{
		String s = new String();
		
		for (int i = 0; i < fieldList.size()-1; i++)
		{
			s += fieldList.get(i).toString() + "|";
		}
		
		if (fieldList.size() > 0)
		{
			s += fieldList.get(fieldList.size() - 1).toString();
		}
		
		return s;
	}

	public String toString(char c)
	{
		String s = new String();
		
		for (int i = 0; i < fieldList.size()-1; i++)
		{
			s += fieldList.get(i).toString() + c;
		}
		
		if (fieldList.size() > 0)
		{
			s += fieldList.get(fieldList.size() - 1).toString();
		}
		
		return s;
	}
}

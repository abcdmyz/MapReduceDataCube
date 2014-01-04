package mrcube.holisticmeasure;

import java.util.ArrayList;

/*
 * encapsulation of ArrayList
 * Tuple is used for multi purpose, as region, group, ...
 */
public class Tuple<T> 
{
	private ArrayList<T> fieldList;
	private int size;
	
	public Tuple(int size)
	{
		this.size = size;
		fieldList = new ArrayList<T>(size);
	}

	public Tuple(Tuple t)
	{
		size = t.size;
		fieldList = (ArrayList<T>) t.fieldList.clone();
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
}

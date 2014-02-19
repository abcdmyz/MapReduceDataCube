package datacube.common;

import java.util.ArrayList;

import datacube.configuration.DataCubeParameter;


public class BatchArea 
{
	private ArrayList<Integer> longestRegionAttributeID; //记录pipesort中最长的region的各个属性
	private ArrayList<Integer> allRegionID; //pipesort中计算的各个region
	
	/*
	private String type;
	private int firstRegionID;
	private int batchAreaSize;
	private int prefixAttributeSize;
	*/
	
	public BatchArea()
	{
		longestRegionAttributeID = new  ArrayList<Integer>();
		allRegionID = new ArrayList<Integer>();
	}
	
	/*
	public BatchArea(String type)
	{
		longestRegionAttributeID = new  ArrayList<Integer>();
		allRegionID = new ArrayList<Integer>();
		this.setType(type);
	}
	
	public BatchArea(int firstRegionID, int batchAreaSize)
	{
		longestRegionAttributeID = new  ArrayList<Integer>();
		allRegionID = new ArrayList<Integer>();
		this.firstRegionID = firstRegionID;
		this.batchAreaSize = batchAreaSize;
	}

	public int getPrefixAttributeSize() 
	{
		return prefixAttributeSize;
	}

	public void setPrefixAttributeSize(int prefixAttributeSize) 
	{
		this.prefixAttributeSize = prefixAttributeSize;
	}

	public int getFirstRegionID() 
	{
		return firstRegionID;
	}

	public void setFirstRegionID(int firstRegionID) 
	{
		this.firstRegionID = firstRegionID;
	}

	public int getBatchAreaSize() 
	{
		return batchAreaSize;
	}

	public void setBatchAreaSize(int batchAreaSize) 
	{
		this.batchAreaSize = batchAreaSize;
	}
	
	public String getType() 
	{
		return type;
	}

	public void setType(String type) 
	{
		this.type = type;
	}
	*/

	public ArrayList<Integer> getLongestRegionAttributeID() 
	{
		return longestRegionAttributeID;
	}

	public void setLongestRegionAttributeID(ArrayList<Integer> longestRegionAttributeID) 
	{
		this.longestRegionAttributeID = longestRegionAttributeID;
	}

	public ArrayList<Integer> getallRegionID() 
	{
		return allRegionID;
	}

	public void setallRegionID(ArrayList<Integer> allRegionID) 
	{
		this.allRegionID = allRegionID;
	}
	
	public void addRegionAttribute(int id)
	{
		longestRegionAttributeID.add(id);
	}
	
	public int getRegionAttribute(int index)
	{
		return longestRegionAttributeID.get(index);
		
	}
	
	public int getRegionID(int index)
	{
		return allRegionID.get(index);
	}

	public void addRegionID(int id)
	{
		allRegionID.add(id);
	}

	
	public int getlongestRegionAttributeSize()
	{
		return longestRegionAttributeID.size();
	}
	
	public int getallRegionIDSize()
	{
		return allRegionID.size();
	}
 }

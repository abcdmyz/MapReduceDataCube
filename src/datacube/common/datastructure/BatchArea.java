package datacube.common.datastructure;

import java.util.ArrayList;

import datacube.configuration.DataCubeParameter;


public class BatchArea 
{
	private ArrayList<Integer> longestRegionAttributeID; //record the attribute ID of longgest region in pipesort
	private ArrayList<Integer> allRegionID; //record all region ID in the pipesort
	
	public BatchArea()
	{
		longestRegionAttributeID = new  ArrayList<Integer>();
		allRegionID = new ArrayList<Integer>();
	}
	
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

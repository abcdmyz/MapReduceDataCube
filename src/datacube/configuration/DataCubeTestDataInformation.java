package datacube.configuration;

import java.util.ArrayList;

import datacube.common.datastructure.Tuple;


public class DataCubeTestDataInformation 
{
	private int attributeSize;
	private int groupAttributeSize;
	private ArrayList<Tuple<Integer>> attributeCubeRollUp;
	
	public int getAttributeSize() 
	{
		return attributeSize;
	}
	public void setAttributeSize(int attributeSize) 
	{
		this.attributeSize = attributeSize;
	}
	
	public ArrayList<Tuple<Integer>> getAttributeCubeRollUp() 
	{
		return attributeCubeRollUp;
	}
	public void setAttributeCubeRollUp(ArrayList<Tuple<Integer>> attributeCubeRollUp) 
	{
		this.attributeCubeRollUp = attributeCubeRollUp;
	}
	
	public int getGroupAttributeSize() 
	{
		return groupAttributeSize;
	}
	public void setGroupAttributeSize(int groupAttributeSize) 
	{
		this.groupAttributeSize = groupAttributeSize;
	}
	
	
}

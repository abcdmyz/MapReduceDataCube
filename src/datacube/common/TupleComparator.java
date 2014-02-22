package datacube.common;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple>  
{

	@Override
	public int compare(Tuple t1, Tuple t2) 
	{
		int s1 = t1.getSize();
		int s2 = t2.getSize();
		
		int i = 0;
		int j = 0;
		
		while ( i < s1 && j < s2 && t1.getFieldString(i).equals(t2.getFieldString(i)))
		{
			i++;
			j++;
		}
		
		if (i >= s1 && j >= s2)
		{
			return 0;
		}
		
		if (i >= s1)
		{
			return 1;
		}
		
		if (j >= s2)
		{
			return -1;
		}
		
		return t2.getFieldString(j).compareTo(t1.getFieldString(i));
	}
}

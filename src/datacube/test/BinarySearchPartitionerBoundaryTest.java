package datacube.test;

import java.util.ArrayList;

public class BinarySearchPartitionerBoundaryTest 
{
	private static ArrayList<String> boundary = new ArrayList<String>(10);
	
	public static void exec()
	{
		boundary.add("bb");
		boundary.add("ff");
		boundary.add("pp");
		boundary.add("tt");
		boundary.add("ww");

		System.out.println(search("a"));
		System.out.println(search("b"));
		System.out.println(search("c"));
		System.out.println(search("g"));
		System.out.println(search("p"));
		System.out.println(search("u"));
		System.out.println(search("z"));
	}

	private static int search(String target)
	{
		int head = 0;
		int tail = boundary.size();
		
		if (target.compareTo(boundary.get(0)) <= 0)
		{
			return 0;
		}
		else if (target.compareTo(boundary.get(boundary.size()-1)) > 0)
		{
			return boundary.size();
		}
		
		while (head < tail - 1)
		{
			int mid = (head +  tail) / 2;
			
			if (target.equals(boundary.get(mid)))
			{
				return mid;
			}
			
			if (target.compareTo(boundary.get(mid)) > 0)
			{
				head = mid;
			}
			else
			{
				tail = mid;
			}
		}
		
		return tail;
	}

}

package filter;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.enhype.extract.EntitySiteTuple;

public class testCustemTuple {
	
	@Test
	public void testTuple(){
		
		EntitySiteTuple tuple1 = new EntitySiteTuple("Hong_Kong","S1");
		EntitySiteTuple tuple2 = new EntitySiteTuple("Singapore","S2");
		
		EntitySiteTuple tuple3 = new EntitySiteTuple("Hong_Kong","S1");
		EntitySiteTuple tuple4 = new EntitySiteTuple("Singapore","S2");
		
		Map<EntitySiteTuple, Integer> testMap = new HashMap<EntitySiteTuple, Integer>();
		testMap.put(tuple1, 5);
		testMap.put(tuple2, 6);
		
		assertTrue(testMap.keySet().size() == 2);
		assertTrue(testMap.get(tuple3) == 5);
		assertTrue(testMap.get(tuple4) == 6);
		
	}

}

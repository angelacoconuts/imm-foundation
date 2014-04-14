package filter;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.enhype.extract.FeatureSiteTuple;

public class testCustemTuple {
	
	@Test
	public void testTuple(){
		
		FeatureSiteTuple tuple1 = new FeatureSiteTuple("Hong_Kong","S1");
		FeatureSiteTuple tuple2 = new FeatureSiteTuple("Singapore","S2");
		
		FeatureSiteTuple tuple3 = new FeatureSiteTuple("Hong_Kong","S1");
		FeatureSiteTuple tuple4 = new FeatureSiteTuple("Singapore","S2");
		
		Map<FeatureSiteTuple, Integer> testMap = new HashMap<FeatureSiteTuple, Integer>();
		testMap.put(tuple1, 5);
		testMap.put(tuple2, 6);
		
		assertTrue(testMap.keySet().size() == 2);
		assertTrue(testMap.get(tuple3) == 5);
		assertTrue(testMap.get(tuple4) == 6);
		
	}

}

package tests.nc.isi.jdbc_sync.utils;

import static java.util.Collections.singleton;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import nc.isi.jdbc_sync.utils.ConcatIterable;

import org.junit.Test;

public class TestConcatIterator {

	@Test
	public void testListsWithElements() {
		ConcatIterable<String> iterable = build("a", "b", "c");
		Iterator<String> iterator = iterable.iterator();
		
		assertEquals("a", iterator.next());
		assertEquals("b", iterator.next());
		assertEquals("c", iterator.next());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testEmptyLists() throws Exception {
		ConcatIterable<String> iterable = build(null, null, null);
		Iterator<String> iterator = iterable.iterator();
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testMixedEmptyOrNot() throws Exception {
		ConcatIterable<String> iterable;
		Iterator<String> iterator;
		
		iterable = build(null, "a", "b");
		iterator = iterable.iterator();
		
		assertEquals("a", iterator.next());
		assertEquals("b", iterator.next());
		assertFalse(iterator.hasNext());
		
		iterable = build("a", null, "b");
		iterator = iterable.iterator();
		
		assertEquals("a", iterator.next());
		assertEquals("b", iterator.next());
		assertFalse(iterator.hasNext());
		
		iterable = build("a", "b", null);
		iterator = iterable.iterator();
		
		assertEquals("a", iterator.next());
		assertEquals("b", iterator.next());
		assertFalse(iterator.hasNext());
	}
	
	private ConcatIterable<String> build(String... elements) {
		List<Iterable<String>> iterables = new ArrayList<>(elements.length);
		for (String element : elements) {
			if (element == null) {
				iterables.add(empty());
			} else {
				iterables.add(singleton(element));
			}
		}
		return new ConcatIterable<String>(iterables);
	}
	
	private Iterable<String> empty() {
		return Collections.emptySet();
	}

}

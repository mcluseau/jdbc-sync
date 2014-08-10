package nc.isi.jdbc_sync.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ConcatIterable<T> implements Iterable<T> {

	@SafeVarargs
	public static <T> ConcatIterable<T> build(Iterable<T>... elements) {
		return new ConcatIterable<>(elements);
	}

	private Collection<Iterable<T>> elements;

	@SafeVarargs
	public ConcatIterable(Iterable<T>... elements) {
		this(Arrays.asList(elements));
	}

	public ConcatIterable(Collection<Iterable<T>> iterables) {
		elements = iterables;
	}

	@Override
	public Iterator<T> iterator() {
		List<Iterator<T>> iterators = new ArrayList<>(elements.size());
		for (Iterable<T> iterable : elements) {
			iterators.add(iterable.iterator());
		}
		return new ConcatIterator<T>(iterators);
	}

}

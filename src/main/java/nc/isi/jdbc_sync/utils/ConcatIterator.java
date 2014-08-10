package nc.isi.jdbc_sync.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ConcatIterator<T> implements Iterator<T> {

	private Iterator<Iterator<T>> iterators;

	private Iterator<T> currentIterator;

	public ConcatIterator(Iterator<Iterator<T>> iterators) {
		this.iterators = iterators;
		currentIterator = iterators.hasNext() ? iterators.next() : null;
	}

	public ConcatIterator(Iterable<Iterator<T>> iterable) {
		this(iterable.iterator());
	}

	@Override
	public boolean hasNext() {
		if (currentIterator == null) {
			return false;
		}
		while (!currentIterator.hasNext()) {
			if (!iterators.hasNext()) {
				return false;
			}
			currentIterator = iterators.next();
		}
		return currentIterator.hasNext();
	}

	@Override
	public T next() {
		if (currentIterator == null) {
			throw new NoSuchElementException();
		}
		while (!currentIterator.hasNext()) {
			currentIterator = iterators.next();
		}
		return currentIterator.next();
	}

	@Override
	public void remove() {
		if (currentIterator == null) {
			throw new IllegalStateException();
		}
		currentIterator.remove();
	}

}

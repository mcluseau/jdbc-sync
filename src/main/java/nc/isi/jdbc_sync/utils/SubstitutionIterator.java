package nc.isi.jdbc_sync.utils;

import java.util.Iterator;

public class SubstitutionIterator<T> implements Iterator<T> {

	private final Iterator<?> delegate;
	private final ValueTranslator<T> translator;

	public SubstitutionIterator(final Iterator<?> delegate,
			final T substitutionValue) {
		this(delegate, new StaticValueTranslator<T>(substitutionValue));
	}

	public SubstitutionIterator(final Iterator<?> delegate,
			final ValueTranslator<T> translator) {
		this.delegate = delegate;
		this.translator = translator;
	}

	public boolean hasNext() {
		return delegate.hasNext();
	}

	public T next() {
		return translator.translate(delegate.next());
	}

	public void remove() {
		// could also be unsupported, but if called, the semantics should
		// translate this way.
		delegate.remove();
	}

}

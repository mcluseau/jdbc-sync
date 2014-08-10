package nc.isi.jdbc_sync.utils;

import java.util.Iterator;

public class SubstitutionIterable<T> implements Iterable<T> {

	private final Iterable<?> values;
	private final ValueTranslator<T> translator;

	public SubstitutionIterable(final Iterable<?> values,
			final T substitutionValue) {
		this(values, new StaticValueTranslator<T>(substitutionValue));
	}

	public SubstitutionIterable(Iterable<?> values,
			ValueTranslator<T> translator) {
		this.values = values;
		this.translator = translator;
	}

	public Iterator<T> iterator() {
		return new SubstitutionIterator<T>(values.iterator(), translator);
	}

}

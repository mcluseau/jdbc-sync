package nc.isi.jdbc_sync.utils;

public class StaticValueTranslator<T> implements ValueTranslator<T> {

	private final T value;

	public StaticValueTranslator(final T value) {
		this.value = value;
	}

	public T translate(Object value) {
		return this.value;
	}

}

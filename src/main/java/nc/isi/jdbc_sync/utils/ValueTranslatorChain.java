package nc.isi.jdbc_sync.utils;

public class ValueTranslatorChain<T> implements ValueTranslator<T> {
	
	private ValueTranslator<T>[] translators;

	@SafeVarargs
	public ValueTranslatorChain(ValueTranslator<T>... translators) {
		if (translators.length == 0) {
			throw new RuntimeException("At least one translator is required.");
		}
		this.translators = translators;
	}

	@Override
	public T translate(Object value) {
		T translation = translators[0].translate(value);
		for (int i = 1; i < translators.length; i++) {
			translation = translators[i].translate(translation);
		}
		return translation;
	}

}

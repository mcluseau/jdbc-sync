package nc.isi.jdbc_sync.utils;

import java.util.LinkedList;
import java.util.List;

public class Translators {
	
	public static final <T> List<T> translate(Iterable<?> values, ValueTranslator<T> translator) {
		List<T> translatedValues = new LinkedList<>();
		for (Object o : values) {
			translatedValues.add(translator.translate(o));
		}
		return translatedValues;
	}
	
	@SafeVarargs
	public static final <T> ValueTranslator<T> chain(ValueTranslator<T>... translators) {
		return new ValueTranslatorChain<T>(translators);
	}

	public static final ValueTranslator<String> EQUALS_PARAM = new ValueTranslator<String>() {
		public String translate(Object value) {
			return value + "=?";
		}
	};

}

package nc.isi.jdbc_sync.utils;

public interface ValueTranslator<T> {
	
	T translate(Object value);

}

package nc.isi.jdbc_sync;

public interface JdbcValueReaderProvider {

	public JdbcValueReader getValueReaderFor(String targetTableName);

}

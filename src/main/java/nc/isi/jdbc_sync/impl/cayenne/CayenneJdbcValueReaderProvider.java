package nc.isi.jdbc_sync.impl.cayenne;

import nc.isi.jdbc_sync.JdbcValueReader;
import nc.isi.jdbc_sync.JdbcValueReaderProvider;

import org.apache.cayenne.map.DataMap;

public class CayenneJdbcValueReaderProvider implements JdbcValueReaderProvider {

	private DataMap dataMap;

	public CayenneJdbcValueReaderProvider(DataMap dataMap) {
		this.dataMap = dataMap;
	}

	@Override
	public JdbcValueReader getValueReaderFor(String targetTableName) {
		return new CayenneJdbcValueReader(dataMap.getDbEntity(targetTableName));
	}

}

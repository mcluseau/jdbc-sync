package nc.isi.jdbc_sync.impl.cayenne;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import nc.isi.jdbc_sync.JdbcValueReader;

import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;

public class CayenneJdbcValueReader implements JdbcValueReader {

	private final DbEntity dbEntity;

	public CayenneJdbcValueReader(DbEntity dbEntity) {
		this.dbEntity = dbEntity;
	}

	@Override
	public Object readValue(ResultSet rs, int col, String colName)
			throws SQLException {
		switch (attribute(colName).getType()) {
		case Types.BIT:
		case Types.BOOLEAN:
			return rs.getBoolean(col);
		default:
			return rs.getObject(col);
		}
	}

	private DbAttribute attribute(String colName) {
		return (DbAttribute) dbEntity.getAttribute(colName);
	}

}

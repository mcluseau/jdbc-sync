package nc.isi.jdbc_sync;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface JdbcValueReader {

	Object readValue(ResultSet rs, int col, String colName) throws SQLException;

}

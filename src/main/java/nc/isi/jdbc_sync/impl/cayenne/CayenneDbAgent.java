package nc.isi.jdbc_sync.impl.cayenne;

import static nc.isi.jdbc_sync.utils.StringUtils.join;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import javax.sql.DataSource;

import nc.isi.jdbc_sync.DbAgent;
import nc.isi.jdbc_sync.model.AssociationTable;
import nc.isi.jdbc_sync.utils.SubstitutionIterable;
import nc.isi.jdbc_sync.utils.Translators;

import org.apache.cayenne.dba.AutoAdapter;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.apache.log4j.Logger;

public class CayenneDbAgent implements DbAgent {

	private final Logger log = Logger.getLogger(getClass());

	private final DataSource dataSource;
	private final AutoAdapter adapter;
	private Connection connection = null;

	public CayenneDbAgent(DataSource dataSource) {
		this.dataSource = dataSource;
		adapter = new AutoAdapter(dataSource);
	}

	public void createAssociationTableIfNotExists(
			AssociationTable associationTable) {
		DbEntity entity = new DbEntity(associationTable.getName());

		if (associationTable.isIncludeTableName()) {
			entity.addAttribute(inPK(varcharColumn("table_name", 255)));
		}
		entity.addAttribute(inPK(hashColumn("pk_hash")));
		entity.addAttribute(hashColumn("data_hash"));

		trySql(adapter.createTable(entity));
	}

	private void trySql(String sql) {
		try {
			Statement stmt = connection().createStatement();
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			safeClose(connection);
		}
	}

	/**
	 * @return lazy init work connection
	 */
	public Connection connection() {
		if (connection == null) {
			try {
				connection = dataSource.getConnection();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return connection;
	}

	private void safeClose(Connection connection) {
	}

	private DbAttribute inPK(DbAttribute attr) {
		attr.setPrimaryKey(true);
		return attr;
	}

	private DbAttribute hashColumn(String attributeName) {
		// The hash is SHA1 => 160 bit => 20 bytes => 40 hexchars.
		return varcharColumn(attributeName, 40);
	}

	private DbAttribute varcharColumn(String attributeName, int length) {
		DbAttribute attribute = new DbAttribute(attributeName);
		attribute.setType(Types.VARCHAR);
		attribute.setMaxLength(length);
		attribute.setMandatory(true);
		return attribute;
	}

	public PreparedStatement prepareSelectStatement(String tableName,
			Iterable<String> columns) {
		return prepareSelectStatement(tableName, columns, null);
	}

	public PreparedStatement prepareSelectStatement(String tableName,
			Iterable<String> columns, String whereClause) {
		StringBuilder sql = new StringBuilder();

		sql.append("SELECT ");

		if (!join(columns, ", ", sql)) {
			throw new RuntimeException("No columns selected");
		}

		sql.append(" FROM ").append(tableName);

		if (whereClause != null) {
			sql.append("WHERE ").append(whereClause);
		}
		return prepare(sql, true);
	}

	private PreparedStatement prepare(StringBuilder sql, boolean scrollable) {
		return prepare(sql.toString(), scrollable);
	}

	private PreparedStatement prepare(String sql, boolean scrollable) {
		try {
			if (scrollable) {
				return connection().prepareStatement(sql,
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
			} else {
				return connection().prepareStatement(sql);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public PreparedStatement prepareInsertStatement(String tableName,
			Iterable<String> columns) {
		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ").append(tableName).append(" (");
		if (!join(columns, ", ", sql)) {
			throw new RuntimeException("No columns to insert");
		}
		sql.append(") VALUES (");
		join(substitute(columns, "?"), ", ", sql);
		sql.append(")");
		return prepare(sql, false);
	}

	private <T> Iterable<T> substitute(Iterable<?> values, T substitutionValue) {
		return new SubstitutionIterable<T>(values, substitutionValue);
	}

	public PreparedStatement prepareUpdateStatement(String tableName,
			Iterable<String> columns, Iterable<String> primaryKey) {
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ").append(tableName).append(" SET ");
		// Append the "column=?" update clauses
		boolean joined = join(substituteWithEqualParam(columns), ", ", sql);
		if (!joined) {
			throw new RuntimeException("No columns to update");
		}
		// append the where clause
		sql.append(" WHERE ");
		join(substituteWithEqualParam(primaryKey), " AND ", sql);
		//
		return prepare(sql, false);
	}

	private SubstitutionIterable<String> substituteWithEqualParam(
			Iterable<String> strings) {
		return new SubstitutionIterable<String>(strings,
				Translators.EQUALS_PARAM);
	}

	public PreparedStatement prepareDeleteStatement(String tableName,
			Iterable<String> columns) {
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ").append(tableName);

		StringBuilder whereClause = new StringBuilder();
		if (join(substituteWithEqualParam(columns), " AND ", whereClause)) {
			sql.append(" WHERE ").append(whereClause.toString());
		}
		return prepare(sql, false);
	}

	@Override
	public void commit() {
		if (connection != null) {
			try {
				if (!connection.getAutoCommit()) {
					connection.commit();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void cleanup() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				log.error(e);
			} finally {
				connection = null;
			}
		}
	}

}

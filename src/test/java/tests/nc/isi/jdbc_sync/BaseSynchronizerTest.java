package tests.nc.isi.jdbc_sync;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import javax.sql.DataSource;

import nc.isi.jdbc_sync.Synchronizer;
import nc.isi.jdbc_sync.impl.cayenne.CayenneDbAgent;
import nc.isi.jdbc_sync.impl.cayenne.CayenneJdbcValueReaderProvider;
import nc.isi.jdbc_sync.impl.cayenne.CayenneSyncDescriptionFactory;
import nc.isi.jdbc_sync.model.SyncDescription;

import org.apache.cayenne.map.Attribute;
import org.apache.cayenne.map.DataMap;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.apache.commons.dbcp.BasicDataSource;

/**
 * 
 * @author Mikaël Cluseau
 *
 */
public class BaseSynchronizerTest {

	private DataSource dataSource;
	private DataMap dataMap;
	private SyncDescription syncDesc;

	protected DataSource dataSource() {
		if (dataSource == null) {
			dataSource = connect(
					"jdbc:h2:mem:jdbc-sync-test;DB_CLOSE_DELAY=-1", "sa", "");
		}
		return dataSource;
	}

	protected void createTables() throws SQLException {
		createTable("source_table");
		createTable("target_table");
	}

	private void createTable(String name) throws SQLException {
		sql("CREATE TABLE IF NOT EXISTS " + name
				+ " (id integer, label varchar(55))");
	}

	protected void performStandardTest(Synchronizer synchronizer)
			throws SQLException, InterruptedException {
		createTables();
		String sourceTable = syncDesc.getSourceTable();
		String targetTable = syncDesc.getTargetTable();

		// Reset state
		sql("delete from " + sourceTable);
		sql("delete from " + targetTable);
		assertCount(0, sourceTable);
		assertCount(0, targetTable);

		// Run with everything empty
		synchronizer.run();

		// Insert in target is reverted
		sql("insert into " + targetTable + " (id, label) values (1, 'Test')");
		assertCount(1, targetTable);
		synchronizer.run();
		assertCount(0, targetTable);

		// Insert in source is applied
		sql("insert into " + sourceTable + " (id, label) values (1, 'Test')");
		assertCount(1, sourceTable);

		synchronizer.run();
		assertCount(1, targetTable);
		assertEquals("Test", valueFor(1));

		// Update in target is reverted
		sql("update " + targetTable + " set label='Test2'");
		assertEquals("Test2", valueFor(1));
		synchronizer.run();
		assertEquals("Test", valueFor(1));

		// Update in source is applied
		sql("update " + sourceTable + " set label='Test2'");
		synchronizer.run();
		assertEquals("Test2", valueFor(1));

		// Delete in target is reverted
		sql("delete from " + targetTable);
		synchronizer.run();
		assertEquals("Test2", valueFor(1));

		// Delete in source is applied
		sql("delete from " + sourceTable);
		synchronizer.run();
		assertCount(0, targetTable);
	}

	protected void dumpTable(String table) throws SQLException {
		Connection conn = dataSource().getConnection();
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("select id, label from " + table);
		System.err.println("table " + table + ":");
		try {
			while (rs.next()) {
				System.err.printf("%3d | %s\n", rs.getInt(1), rs.getString(2));
			}
		} finally {
			rs.close();
			st.close();
			conn.close();
		}
	}

	private void assertCount(long count, String table) throws SQLException {
		assertEquals(count, scalar("select count(*) from " + table));
	}

	private Object valueFor(int id) throws SQLException {
		return scalar("select label from " + syncDesc.getTargetTable()
				+ " where id=" + id);
	}

	private Object scalar(String sql) throws SQLException {
		Connection conn = dataSource().getConnection();
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(sql);
		try {
			if (rs.next()) {
				Object value = rs.getObject(1);
				if (rs.next()) {
					throw new AssertionError("More than 1 result for: " + sql);
				}
				return value;
			}
		} finally {
			rs.close();
			st.close();
			conn.close();
		}
		throw new AssertionError("No result for: " + sql);
	}

	private void sql(String sql) throws SQLException {
		Connection conn = dataSource().getConnection();
		Statement st = conn.createStatement();
		try {
			st.execute(sql);
		} finally {
			st.close();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			conn.close();
		}
	}

	protected void setupSynchronizer(Synchronizer synchronizer) {
		DataSource dataSource = dataSource();

		syncDesc = CayenneSyncDescriptionFactory.create(dataMap().getDbEntity(
				"target_table"));
		syncDesc.setSourceTable("source_table");

		synchronizer.setSourceDbAgent(new CayenneDbAgent(dataSource));
		synchronizer.setTargetDbAgent(new CayenneDbAgent(dataSource));
		synchronizer.setSyncToPerform(syncDesc.instanciate(dataSource,
				dataSource));
	}

	protected CayenneJdbcValueReaderProvider valueReader() {
		return new CayenneJdbcValueReaderProvider(dataMap());
	}

	protected DataMap dataMap() {
		if (dataMap == null) {
			DbEntity targetTable = new DbEntity("target_table");
			targetTable.addAttribute(attr("id", Types.INTEGER));
			targetTable.addAttribute(attr("label", Types.VARCHAR, 55));
			((DbAttribute) targetTable.getAttribute("id")).setPrimaryKey(true);

			dataMap = new DataMap();
			dataMap.addDbEntity(targetTable);
		}
		return dataMap;
	}

	private Attribute attr(String name, int type) {
		return attr(name, type, -1);
	}

	private DbAttribute attr(String name, int type, int maxLength) {
		DbAttribute attr = new DbAttribute(name);
		attr.setType(type);
		if (maxLength >= 0) {
			attr.setMaxLength(maxLength);
		}
		return attr;
	}

	protected DataSource connect(String connectUri, String userName,
			String password) {
		BasicDataSource ds = new BasicDataSource();
		ds.setUrl(connectUri);
		ds.setUsername(userName);
		ds.setPassword(password);
		return ds;
	}

}

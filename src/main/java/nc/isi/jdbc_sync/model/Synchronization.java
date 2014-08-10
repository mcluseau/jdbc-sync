package nc.isi.jdbc_sync.model;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class Synchronization {

	private final SyncDescription model;
	private final DataSource sourceDataSource;
	private final DataSource targetDataSource;

	public Synchronization(SyncDescription model, DataSource sourceDataSource,
			DataSource targetDataSource) {
		this.model = model;
		this.sourceDataSource = sourceDataSource;
		this.targetDataSource = targetDataSource;
	}

	public SyncDescription getModel() {
		return model;
	}

	/**
	 * Returns a connection to the source database. The caller is responsible of
	 * closing it.
	 * 
	 * @return A connection to the source database.
	 */
	public Connection getConnectionToSource() {
		try {
			return sourceDataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns a connection to the target database. The caller is responsible of
	 * closing it.
	 * 
	 * @return A connection to the target database.
	 */
	public Connection getConnectionToTarget() {
		try {
			return targetDataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}

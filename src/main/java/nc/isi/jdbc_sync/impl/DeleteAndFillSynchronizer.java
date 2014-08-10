package nc.isi.jdbc_sync.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import nc.isi.jdbc_sync.DbAgent;
import nc.isi.jdbc_sync.JdbcSyncConfig;
import nc.isi.jdbc_sync.JdbcValueReader;
import nc.isi.jdbc_sync.JdbcValueReaderProvider;
import nc.isi.jdbc_sync.Synchronizer;
import nc.isi.jdbc_sync.model.SyncDescription;
import nc.isi.jdbc_sync.model.Synchronization;

public class DeleteAndFillSynchronizer implements Synchronizer {

	private Logger log = Logger.getLogger(getClass());

	// private Synchronization sync;
	private SyncDescription model;
	private DbAgent sourceDbAgent;
	private DbAgent targetDbAgent;

	private JdbcValueReaderProvider jdbcValueReaderProvider;

	private final JdbcSyncConfig config;

	public DeleteAndFillSynchronizer(JdbcSyncConfig config,
			JdbcValueReaderProvider jdbcValueReaderProvider) {
		this.config = config;
		this.jdbcValueReaderProvider = jdbcValueReaderProvider;
	}

	public void setSourceDbAgent(DbAgent sourceDbAgent) {
		this.sourceDbAgent = sourceDbAgent;
	}

	public void setTargetDbAgent(DbAgent targetDbAgent) {
		this.targetDbAgent = targetDbAgent;
	}

	public void setSyncToPerform(Synchronization synchronization) {
		// this.sync = synchronization;
		this.model = synchronization.getModel();
	}

	public void run() {
		try {
			emptyTargetTable();
			fillTargetTable();
			sourceDbAgent.cleanup();
			targetDbAgent.cleanup();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private void emptyTargetTable() throws SQLException {
		log.debug("Flushing target table...");

		PreparedStatement delete = targetDbAgent.prepareDeleteStatement(
				model.getTargetTable(), new LinkedList<String>());
		delete.execute();
		delete.close();
		targetDbAgent.commit();
	}

	private void fillTargetTable() throws SQLException {
		log.debug("Filling target table...");

		int batchSize = config.getBatchSize(model.getTargetTable());

		JdbcValueReader jdbcValueReader = jdbcValueReaderProvider
				.getValueReaderFor(model.getTargetTable());

		Iterable<String> sourceColumns = model.getSourceColumns();

		PreparedStatement select = sourceDbAgent.prepareSelectStatement(
				model.getSourceTable(), sourceColumns);
		PreparedStatement insert = targetDbAgent.prepareInsertStatement(
				model.getTargetTable(), model.getTargetColumns());

		int currentBatchSize = 0;
		ResultSet rs = select.executeQuery();
		while (rs.next()) {
			int col = 0;
			for (String colName : sourceColumns) {
				col++;
				Object value = jdbcValueReader.readValue(rs, col, colName);
				insert.setObject(col, value);
			}
			insert.addBatch();
			currentBatchSize++;

			if (currentBatchSize >= batchSize) {
				insert.executeBatch();
				currentBatchSize = 0;
				targetDbAgent.commit();
			}
		}
		if (currentBatchSize > 0) {
			insert.executeBatch();
			targetDbAgent.commit();
		}
		insert.close();
	}

}

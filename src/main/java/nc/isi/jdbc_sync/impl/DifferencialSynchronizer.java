package nc.isi.jdbc_sync.impl;

import static nc.isi.jdbc_sync.utils.Translators.translate;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import nc.isi.jdbc_sync.DbAgent;
import nc.isi.jdbc_sync.JdbcSyncConfig;
import nc.isi.jdbc_sync.JdbcValueReader;
import nc.isi.jdbc_sync.JdbcValueReaderProvider;
import nc.isi.jdbc_sync.Synchronizer;
import nc.isi.jdbc_sync.model.SyncDescription;
import nc.isi.jdbc_sync.model.Synchronization;
import nc.isi.jdbc_sync.utils.ConcatIterable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

public class DifferencialSynchronizer implements Synchronizer {

	private final Logger log = Logger.getLogger(getClass());

	private DbAgent sourceDbAgent;
	private DbAgent targetDbAgent;

	// private Synchronization sync;
	private SyncDescription desc;

	// private JdbcSyncConfig syncConfig;

	private JdbcValueReaderProvider jdbcValueReaderProvider;

	private JdbcValueReader jdbcValueReader;

	public DifferencialSynchronizer(JdbcValueReader jdbcValueReader) {
		this.jdbcValueReader = jdbcValueReader;
	}

	public DifferencialSynchronizer(JdbcSyncConfig syncConfig,
			JdbcValueReaderProvider jdbcValueReaderProvider) {
		// this.syncConfig = syncConfig;
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
		desc = synchronization.getModel();
	}

	public void run() {
		jdbcValueReader = jdbcValueReaderProvider.getValueReaderFor(desc
				.getTargetTable());

		final List<String> sourceColumns = allSourceColumns();
		FutureTask<TableHash> futureSourceHash = futureSourceHash(sourceColumns);
		FutureTask<TableHash> futureTargetHash = futureTargetHash(sourceColumns);

		PreparedStatement deleteStatement = makeDeleteStatement();
		PreparedStatement insertStatement = makeInsertStatement();
		PreparedStatement updateStatement = makeUpdateStatement();

		try {
			TableHash sourceHash = futureSourceHash.get();
			TableHash targetHash = futureTargetHash.get();

			for (String pkHash : sourceHash.pks()) {
				if (targetHash.hasPk(pkHash)) {
					if (Objects.equals(sourceHash.dataHashFor(pkHash),
							targetHash.dataHashFor(pkHash))) {
						// same => skip
						continue;
					}
					updateByHash(updateStatement, sourceHash, pkHash);
				} else {
					// missing in target => create in target
					insertByHash(insertStatement, sourceHash, pkHash);
				}
			}

			for (String pkHash : targetHash.pks()) {
				if (!sourceHash.hasPk(pkHash)) {
					// missing in source => delete in target
					deleteByHash(deleteStatement, targetHash, pkHash);
				}
			}

			sourceHash.close();
			targetHash.close();

			targetDbAgent.commit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			jdbcValueReader = null;
			sourceDbAgent.cleanup();
			targetDbAgent.cleanup();
		}
	}

	private PreparedStatement makeInsertStatement() {
		return targetDbAgent.prepareInsertStatement(desc.getTargetTable(),
				desc.getTargetColumns());
	}

	private void insertByHash(PreparedStatement insertStatement,
			TableHash sourceHash, String pkHash) throws SQLException {
		log.debug("Inserting (hash " + pkHash + ")");
		sourceHash.moveToHash(pkHash);
		int i = 0;
		for (Object value : sourceHash.readData()) {
			i++;
			insertStatement.setObject(i, value);
		}
		insertStatement.execute();
	}

	private PreparedStatement makeUpdateStatement() {
		return targetDbAgent.prepareUpdateStatement(desc.getTargetTable(),
				desc.getTargetColumns(), desc.getPrimaryKeyInTarget());
	}

	private void updateByHash(PreparedStatement updateStatement,
			TableHash sourceHash, String pkHash) throws SQLException {
		log.debug("Updating (hash " + pkHash + ")");
		sourceHash.moveToHash(pkHash);
		int i = 0;
		for (Object value : ConcatIterable.build(sourceHash.readData(),
				sourceHash.readPk())) {
			i++;
			updateStatement.setObject(i, value);
		}
		updateStatement.execute();
	}

	private PreparedStatement makeDeleteStatement() {
		return targetDbAgent.prepareDeleteStatement(desc.getTargetTable(),
				desc.getPrimaryKeyInTarget());
	}

	private void deleteByHash(PreparedStatement deleteStatement,
			TableHash targetHash, String pkHash) throws SQLException {
		log.debug("Deleting (hash " + pkHash + ")");
		targetHash.moveToHash(pkHash);
		int i = 0;
		for (Object value : targetHash.readPk()) {
			i++;
			deleteStatement.setObject(i, value);
		}
		deleteStatement.execute();
		// deleteStatement.addBatch();
		// deleteStatement.executeBatch();
	}

	private FutureTask<TableHash> futureSourceHash(
			final List<String> sourceColumns) {
		FutureTask<TableHash> futureSourceHash = new FutureTask<>(
				new Callable<TableHash>() {
					@Override
					public TableHash call() throws Exception {
						PreparedStatement sourceSelect = sourceDbAgent
								.prepareSelectStatement(desc.getSourceTable(),
										sourceColumns);
						return new TableHash(sourceSelect.executeQuery(),
								sourceColumns, desc.getPrimaryKey(), desc
										.getSourceColumns());
					}
				});
		new Thread(futureSourceHash).start();
		return futureSourceHash;
	}

	private FutureTask<TableHash> futureTargetHash(
			final List<String> sourceColumns) {
		FutureTask<TableHash> futureTargetHash = new FutureTask<>(
				new Callable<TableHash>() {
					@Override
					public TableHash call() throws Exception {
						List<String> targetColumns = translate(sourceColumns,
								desc.getColumnMapper());
						PreparedStatement targetSelect = targetDbAgent
								.prepareSelectStatement(desc.getTargetTable(),
										targetColumns);
						return new TableHash(targetSelect.executeQuery(),
								targetColumns, //
								translate(desc.getPrimaryKey(),
										desc.getColumnMapper()), //
								translate(desc.getSourceColumns(),
										desc.getColumnMapper()));
					}
				});
		new Thread(futureTargetHash).start();
		return futureTargetHash;
	}

	/**
	 * @return All source columns: PK first then the remaining columns.
	 */
	private List<String> allSourceColumns() {
		List<String> sourceColumns = new ArrayList<>();
		sourceColumns.addAll(desc.getPrimaryKey());
		for (String col : desc.getSourceColumns()) {
			if (sourceColumns.contains(col)) {
				continue;
			}
			sourceColumns.add(col);
		}
		return sourceColumns;
	}

	private class TableHash {

		private final Map<String, String> pk2data = new TreeMap<String, String>();
		private final Map<String, Integer> pk2rowNum = new TreeMap<String, Integer>();
		private final ColumnHasher pkHasher;
		private final ColumnHasher dataHasher;
		private final ResultSet rs;

		public TableHash(ResultSet rs, List<String> columnsInOrder,
				Iterable<String> pkCols, Iterable<String> dataCols)
				throws SQLException {
			this.rs = rs;
			pkHasher = new ColumnHasher(rs, columnsInOrder, pkCols);
			dataHasher = new ColumnHasher(rs, columnsInOrder, dataCols);
			compute();
		}

		/**
		 * Reads the given underlying ResultSet and produce hashes.
		 * 
		 * @throws SQLException
		 *             On read error.
		 */
		private void compute() throws SQLException {
			while (rs.next()) {
				String pkHash = pkHasher.currentHash();
				pk2data.put(pkHash, dataHasher.currentHash());
				pk2rowNum.put(pkHash, rs.getRow());
			}
			log.debug("Loaded " + pk2data.size() + " hashes.");
		}

		/**
		 * @return The hashes of each primary key of this set.
		 */
		public Set<String> pks() {
			return pk2data.keySet();
		}

		/**
		 * @param pkHash
		 *            The PK hash to look for.
		 * @return <code>true</code> iff the given hash exists in this set.
		 */
		public boolean hasPk(String pkHash) {
			return pk2data.containsKey(pkHash);
		}

		/**
		 * @param pkHash
		 *            The PK hash to look for.
		 * @return The hash of the data associated to the given PK hash.
		 */
		public String dataHashFor(String pkHash) {
			return pk2data.get(pkHash);
		}

		// Direct access functions

		/**
		 * Seeks to the given PK hash.
		 * 
		 * @param pkHash
		 *            The PK hash to seek to.
		 * @throws SQLException
		 *             If the seek was not possible.
		 */
		public void moveToHash(String pkHash) throws SQLException {
			if (!rs.absolute(pk2rowNum.get(pkHash))) {
				throw new SQLException("move failed");
			}
		}

		/**
		 * Reads the PK at the current position.
		 * 
		 * @return The values for the PK columns.
		 * @throws SQLException
		 *             on read error.
		 */
		public List<Object> readPk() throws SQLException {
			return pkHasher.readValues();
		}

		/**
		 * Reads the data at the current position.
		 * 
		 * @return The values for the data columns.
		 * @throws SQLException
		 *             on read error.
		 */
		public List<Object> readData() throws SQLException {
			return dataHasher.readValues();
		}

		/**
		 * Closes the underlying {@link ResultSet}. See
		 * {@link ResultSet#close()}.
		 */
		public void close() throws SQLException {
			rs.close();
		}

	}

	private class ColumnHasher {

		private ResultSet rs;
		private List<Integer> colIndices = new ArrayList<Integer>();
		private Iterable<String> cols;

		public ColumnHasher(ResultSet rs, List<String> columnsInOrder,
				Iterable<String> cols) {
			this.rs = rs;
			this.cols = cols;
			for (String colName : cols) {
				int colIdx = columnsInOrder.indexOf(colName);
				if (colIdx < 0) {
					throw new IllegalArgumentException("column not found: "
							+ colName + " (columns: " + columnsInOrder + ")");
				}
				colIndices.add(colIdx + 1);
			}
		}

		public String currentHash() throws SQLException {
			return DigestUtils.shaHex(SerializationUtils
					.serialize((Serializable) readValues()));
		}

		private List<Object> readValues() throws SQLException {
			ArrayList<Object> values = new ArrayList<Object>(colIndices.size());
			Iterator<Integer> idxIterator = colIndices.iterator();
			for (String colName : cols) {
				int idx = idxIterator.next();
				Object value = jdbcValueReader.readValue(rs, idx, colName);
				values.add(value);
			}
			return values;
		}

	}

}

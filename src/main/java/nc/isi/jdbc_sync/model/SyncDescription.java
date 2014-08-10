package nc.isi.jdbc_sync.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.sql.DataSource;

import nc.isi.jdbc_sync.utils.SubstitutionIterable;
import nc.isi.jdbc_sync.utils.Translators;
import nc.isi.jdbc_sync.utils.ValueTranslator;

public class SyncDescription {

	private String sourceTable;

	private String targetTable;

	private final Map<String, String> columnMapping = new TreeMap<String, String>();

	private final Set<String> primaryKey = new TreeSet<String>();

	private final ValueTranslator<String> columnMapper = new ValueTranslator<String>() {
		@Override
		public String translate(Object value) {
			if (!columnMapping.containsKey(value)) {
				throw new RuntimeException("Column " + value + " not mapped.");
			}
			return columnMapping.get(value);
		}
	};

	public Synchronization instanciate(DataSource sourceDataSource,
			DataSource targetDataSource) {
		return new Synchronization(this, sourceDataSource, targetDataSource);
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public Map<String, String> getColumnMapping() {
		return Collections.unmodifiableMap(columnMapping);
	}

	public void mapColumn(String from, String to) {
		columnMapping.put(from, to);
	}

	public Set<String> getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String... pk) {
		primaryKey.clear();
		primaryKey.addAll(Arrays.asList(pk));
	}

	public void addToPrimaryKey(String pk) {
		primaryKey.add(pk);
	}

	public Iterable<String> getSourceColumns() {
		return columnMapping.keySet();
	}

	public Iterable<String> getTargetColumns() {
		return new SubstitutionIterable<String>(getSourceColumns(),
				new ValueTranslator<String>() {
					public String translate(Object value) {
						return columnMapping.get(value);
					}
				});
	}

	public String mappingFor(String sourceColumn) {
		return columnMapping.get(sourceColumn);
	}

	public ValueTranslator<String> getColumnMapper() {
		return columnMapper;
	}

	public Iterable<String> getPrimaryKeyInTarget() {
		return Translators.translate(getPrimaryKey(), getColumnMapper());
	}

	@Override
	public String toString() {
		return "SyncDescription [sourceTable=" + sourceTable + ", targetTable="
				+ targetTable + ", columnMapping=" + columnMapping
				+ ", primaryKey=" + primaryKey + "]";
	}

}

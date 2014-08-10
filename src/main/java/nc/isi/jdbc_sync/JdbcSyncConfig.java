package nc.isi.jdbc_sync;

public interface JdbcSyncConfig {
	
	public static final JdbcSyncConfig DEFAULT = new JdbcSyncConfig() {
		public int getBatchSize(String targetTable) {
			return 1000;
		}
	};

	public int getBatchSize(String targetTable);

}

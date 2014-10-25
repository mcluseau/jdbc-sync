package nc.isi.jdbc_sync;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.sql.DataSource;

import nc.isi.jdbc_sync.impl.DifferencialSynchronizer;
import nc.isi.jdbc_sync.impl.cayenne.CayenneDbAgent;
import nc.isi.jdbc_sync.impl.cayenne.CayenneJdbcValueReaderProvider;
import nc.isi.jdbc_sync.impl.cayenne.CayenneSyncDescriptionFactory;
import nc.isi.jdbc_sync.model.SyncDescription;

import org.apache.cayenne.access.DataDomain;
import org.apache.cayenne.conf.Configuration;
import org.apache.cayenne.map.DataMap;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.mock.jndi.SimpleNamingContextBuilder;

/**
 * Main class for syncing a table couple from the command line.
 * 
 * <p>
 * Uses a Cayenne domain (that must be in the classpath) and the
 * {@link DifferencialSynchronizer}.
 * 
 * <p>
 * <code>java -cp jdbc-sync.jar:my-app.jar nc.isi.jdbc_sync.SyncTable data-sources.properties source_table cayenne_map.target_table</code>
 * 
 * @author Mikaël Cluseau
 * 
 */
public class SyncTable implements Runnable {

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		if (args.length != 3) {
			usage();
			return;
		}

		String dataSourcePropertiesFile = args[0];
		String sourceTable = args[1];

		String[] mapAndTargetTable = args[2].split("\\.", 2);
		String cayenneMapName = mapAndTargetTable[0];
		String targetTable = mapAndTargetTable[1];

		new SyncTable(dataSourcePropertiesFile, sourceTable, //
				cayenneMapName, targetTable).run();
	}

	private static void usage() {
		System.err.println("USAGE: java -cp jdbc-sync.jar:my-app.jar "
				+ "nc.isi.jdbc_sync.SyncTable data-sources.properties "
				+ "source-table-name target-table-name");
	}

	private final Properties dataSourceProperties;

	private DataSource sourceDataSource;
	private DataSource targetDataSource;

	private SimpleNamingContextBuilder namingContextBuilder;

	private SyncDescription syncDesc;

	private DataMap dataMap;

	public SyncTable(String dataSourcePropertiesFile, String sourceTable,
			String cayenneMapName, String targetTable)
			throws FileNotFoundException, IOException {
		dataSourceProperties = new Properties();
		initProperties(dataSourcePropertiesFile);

		sourceDataSource = loadDataSource("source");
		targetDataSource = loadDataSource("target");
		activateJndi();

		initSyncDesc(sourceTable, cayenneMapName, targetTable);
	}

	/**
	 * Initialize the {@link SyncDescription} of this instance.
	 * 
	 * @param sourceTable
	 *            The table to synchronize from.
	 * @param cayenneMapName
	 *            The Cayenne DataMap to look for the target table from.
	 * @param targetTable
	 *            The table to synchronize to.
	 */
	private void initSyncDesc(String sourceTable, String cayenneMapName,
			String targetTable) {

		DataDomain domain = Configuration.getSharedConfiguration().getDomain();
		dataMap = domain.getMap(cayenneMapName);

		syncDesc = CayenneSyncDescriptionFactory.create(dataMap
				.getDbEntity(targetTable));
		syncDesc.setSourceTable(sourceTable);
	}

	/**
	 * Activate the JNDI context if any JNDI named was provided.
	 */
	private void activateJndi() {
		if (namingContextBuilder == null) {
			return;
		}

		try {
			namingContextBuilder.activate();
		} catch (IllegalStateException | NamingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a {@link DataSource} from the properties prefixed by
	 * <code>prefix</code>.
	 * 
	 * @param prefix
	 *            The prefix before each property.
	 * @return A new {@link DataSource}.
	 */
	private DataSource loadDataSource(String prefix) {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(property(prefix + ".driver"));
		dataSource.setUrl(property(prefix + ".url"));
		dataSource.setUsername(property(prefix + ".username"));
		dataSource.setPassword(property(prefix + ".password"));

		String jndiName = property(prefix + ".jndiName");
		bindToJndi(jndiName, dataSource);

		return dataSource;
	}

	/**
	 * Binds the given object to the current JNDI context.
	 */
	private void bindToJndi(String jndiName, Object object) {
		if (jndiName != null) {
			if (namingContextBuilder == null) {
				namingContextBuilder = new SimpleNamingContextBuilder();
			}
			namingContextBuilder.bind(jndiName, object);
		}
	}

	/**
	 * Simply wrap {@link Properties#getProperty(String)} in a shortcut.
	 */
	private String property(String property) {
		return dataSourceProperties.getProperty(property);
	}

	/**
	 * Loads the properties from a given file path.
	 */
	private void initProperties(String dataSourcePropertiesFile)
			throws FileNotFoundException, IOException {
		FileInputStream in = new FileInputStream(dataSourcePropertiesFile);
		dataSourceProperties.load(in);
		in.close();
	}

	@Override
	public void run() {
		Synchronizer synchronizer = new DifferencialSynchronizer(
				JdbcSyncConfig.DEFAULT, new CayenneJdbcValueReaderProvider(
						dataMap));
		synchronizer.setSourceDbAgent(new CayenneDbAgent(sourceDataSource));
		synchronizer.setTargetDbAgent(new CayenneDbAgent(targetDataSource));
		synchronizer.setSyncToPerform(syncDesc.instanciate(sourceDataSource,
				targetDataSource));

		synchronizer.run();
	}

}

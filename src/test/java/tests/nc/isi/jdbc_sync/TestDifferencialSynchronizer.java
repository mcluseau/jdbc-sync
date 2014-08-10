package tests.nc.isi.jdbc_sync;

import nc.isi.jdbc_sync.JdbcSyncConfig;
import nc.isi.jdbc_sync.Synchronizer;
import nc.isi.jdbc_sync.impl.DifferencialSynchronizer;

import org.junit.Test;

public class TestDifferencialSynchronizer extends BaseSynchronizerTest {

	@Test
	public void testRun() throws Exception {
		Synchronizer synchronizer = new DifferencialSynchronizer(
				JdbcSyncConfig.DEFAULT, valueReader());

		setupSynchronizer(synchronizer);

		performStandardTest(synchronizer);
	}

}

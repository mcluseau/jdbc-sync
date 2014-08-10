package tests.nc.isi.jdbc_sync;

import nc.isi.jdbc_sync.JdbcSyncConfig;
import nc.isi.jdbc_sync.Synchronizer;
import nc.isi.jdbc_sync.impl.DeleteAndFillSynchronizer;

import org.junit.Test;

public class TestDeleteAndFillSynchronizer extends BaseSynchronizerTest {

	@Test
	public void testRun() throws Exception {
		Synchronizer synchronizer = new DeleteAndFillSynchronizer(
				JdbcSyncConfig.DEFAULT, valueReader());

		setupSynchronizer(synchronizer);

		performStandardTest(synchronizer);
	}

}

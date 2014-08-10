package nc.isi.jdbc_sync;

import nc.isi.jdbc_sync.model.Synchronization;

/**
 * 
 * Applies a given {@link Synchronization}.
 * 
 * @author Mikaël Cluseau
 * 
 */
public interface Synchronizer extends Runnable {

	/**
	 * Sets the agent to operate on the source database.
	 * 
	 * @param sourceDbAgent
	 *            The agent to operate on the source database.
	 */
	void setSourceDbAgent(DbAgent sourceDbAgent);

	/**
	 * Sets the agent to operate on the target database.
	 * 
	 * @param targetDbAgent
	 *            The agent to operate on the target database.
	 */
	void setTargetDbAgent(DbAgent targetDbAgent);
	
	/**
	 * Sets the synchronisation this synchronizer will have to do.
	 * 
	 * @param synchronization
	 *            The synchronisation this synchronizer will have to do.
	 */
	void setSyncToPerform(Synchronization synchronization);

}

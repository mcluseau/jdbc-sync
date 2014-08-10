package nc.isi.jdbc_sync;

import java.sql.PreparedStatement;

import nc.isi.jdbc_sync.model.AssociationTable;

public interface DbAgent {

	/**
	 * Create the given association table if it does not exists..
	 */
	public void createAssociationTableIfNotExists(
			AssociationTable associationTable);

	/**
	 * Prepare a select statement from the given parameters.
	 * 
	 * <p>
	 * The columns will be in the same order as the <code>columns</code>
	 * parameter.
	 * 
	 * @param tableName
	 *            The name of the table to query.
	 * @param columns
	 *            The name of the columns to return.
	 * @return A {@link PreparedStatement} representing the select.
	 */
	public PreparedStatement prepareSelectStatement(String tableName,
			Iterable<String> columns);

	/**
	 * Prepare a select statement from the given parameters.
	 * 
	 * <p>
	 * The columns will be in the same order as the <code>columns</code>
	 * parameter.
	 * 
	 * @param tableName
	 *            The name of the table to query.
	 * @param columns
	 *            The name of the columns to return.
	 * @param whereClause
	 *            The where clause used to filter the output.
	 * @return A {@link PreparedStatement} representing the select.
	 */
	public PreparedStatement prepareSelectStatement(String tableName,
			Iterable<String> columns, String whereClause);

	/**
	 * Prepare an insert statement from the given parameters.
	 * <p>
	 * The positional parameters will be in the same order as the
	 * <code>columns</code> parameter.
	 * 
	 * @param tableName
	 *            The name of the table to insert to.
	 * @param columns
	 *            The name of the columns to set.
	 * @return A {@link PreparedStatement} representing the insert.
	 */
	public PreparedStatement prepareInsertStatement(String tableName,
			Iterable<String> columns);

	/**
	 * Prepare an update statement from the given parameters.
	 * <p>
	 * The positional parameters will be in the same order as the
	 * <code>columns</code> parameter, followed by the columns in the
	 * <code>primaryKey</code>.
	 * 
	 * @param tableName
	 *            The name of the table to insert to.
	 * @param columns
	 *            The name of the columns to set.
	 * @return A {@link PreparedStatement} representing the insert.
	 */
	public PreparedStatement prepareUpdateStatement(String tableName,
			Iterable<String> columns, Iterable<String> primaryKey);

	/**
	 * Prepare a delete statement from the given parameters.
	 * <p>
	 * The positional parameters will be in the same order as the
	 * <code>columns</code> parameter.
	 * 
	 * @param tableName
	 *            The name of the table to insert to.
	 * @param columns
	 *            The name of the columns to filter the deletion (can be empty).
	 * @return A {@link PreparedStatement} representing the insert.
	 */
	public PreparedStatement prepareDeleteStatement(String tableName,
			Iterable<String> columns);
	
	/**
	 * Commits transaction if needed.
	 */
	public void commit();

	/**
	 * Hook invoked at the end of the synchronisation process.
	 */
	public void cleanup();

}

package nc.isi.jdbc_sync.model;

public class AssociationTable {

	private String name;

	private boolean includeTableName;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isIncludeTableName() {
		return includeTableName;
	}

	public void setIncludeTableName(boolean includeTableName) {
		this.includeTableName = includeTableName;
	}

}

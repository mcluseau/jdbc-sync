package nc.isi.jdbc_sync.impl.cayenne;

import nc.isi.jdbc_sync.model.SyncDescription;

import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;

public class CayenneSyncDescriptionFactory {

	public static SyncDescription create(DbEntity entity) {
		SyncDescription desc = new SyncDescription();

		desc.setSourceTable(entity.getName());
		desc.setTargetTable(entity.getName());

		for (Object o : entity.getPrimaryKey()) {
			DbAttribute attr = (DbAttribute) o;
			desc.addToPrimaryKey(attr.getName());
		}

		for (Object o : entity.getAttributes()) {
			DbAttribute attr = (DbAttribute) o;
			desc.mapColumn(attr.getName(), attr.getName());
		}

		return desc;
	}

}

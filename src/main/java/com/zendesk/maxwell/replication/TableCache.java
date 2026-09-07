package com.zendesk.maxwell.replication;

import java.util.HashMap;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class TableCache {
	private final String maxwellDB;

	public TableCache(String maxwellDB) {
		this.maxwellDB = maxwellDB;
	}
	private final HashMap<Long, Table> tableMapCache = new HashMap<>();

	public void processEvent(Schema schema, Filter filter, Boolean ignoreMissingSchema, Long tableId, String dbName, String tblName) {
		if ( !tableMapCache.containsKey(tableId)) {
			if ( filter.isTableBlacklisted(dbName, tblName) ) {
				return;
			}


			Database db = schema.findDatabase(dbName);
			if ( db == null ) {
				if ( !ignoreMissingSchema || filter.includes(dbName, tblName) )
					throw new RuntimeException("Couldn't find database " + dbName);

			} else {
				Table tbl = db.findTable(tblName);

				if (tbl == null) {
					if ( !ignoreMissingSchema || filter.includes(dbName, tblName) )
						throw new RuntimeException("Couldn't find table " + tblName + " in database " + dbName);

				} else {
					tableMapCache.put(tableId, tbl);
				}
			}
		}

	}

	/**
	 * Cache a table definition carried by a MySQL 8 TABLE_MAP event. Unlike the
	 * persisted-schema path, replace the entry on every event because TABLE_MAP
	 * is the authoritative schema generation for the following row events.
	 */
	public void processEvent(TableMapEventData event, Filter filter) {
		if (filter.isTableBlacklisted(event.getDatabase(), event.getTable())) {
			tableMapCache.remove(event.getTableId());
			return;
		}

		tableMapCache.put(event.getTableId(), BinlogTableMetadata.buildTable(event));
	}

	public Table getTable(Long tableId) {
		return tableMapCache.get(tableId);
	}

	public void clear() {
		tableMapCache.clear();
	}
}

package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDefWithLength;
import com.zendesk.maxwell.schema.columndef.EnumeratedColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BinlogTableMetadataTest {
	@Test
	public void buildsTableFromFullMetadata() {
		TableMapEventData event = event(
			new ColumnType[] {
				ColumnType.LONG,
				ColumnType.LONGLONG,
				ColumnType.VARCHAR,
				ColumnType.BLOB,
				ColumnType.STRING,
				ColumnType.STRING,
				ColumnType.DATETIME_V2,
				ColumnType.GEOMETRY
			},
			new int[] {
				0,
				0,
				255,
				2,
				(ColumnType.ENUM.getCode() << 8) | 1,
				(ColumnType.SET.getCode() << 8) | 1,
				6,
				4
			},
			"unsigned_id", "signed_id", "label", "payload", "state", "flags", "created_at", "point"
		);

		TableMapEventMetadata metadata = event.getEventMetadata();
		BitSet unsignedColumns = new BitSet();
		unsignedColumns.set(0);
		metadata.setSignedness(unsignedColumns);
		metadata.setColumnCharsets(Arrays.asList(255, 63)); // utf8mb4, binary
		metadata.setEnumStrValues(Collections.singletonList(new String[] { "new", "done" }));
		metadata.setSetStrValues(Collections.singletonList(new String[] { "a", "b" }));
		metadata.setGeometryTypes(Collections.singletonList(1));
		metadata.setSimplePrimaryKeys(Collections.singletonList(0));

		Table table = BinlogTableMetadata.buildTable(event);

		assertEquals("orders", table.getName());
		assertEquals(Collections.singletonList("unsigned_id"), table.getPKList());
		assertFalse(((IntColumnDef) table.findColumn(0)).isSigned());
		assertTrue(((BigIntColumnDef) table.findColumn(1)).isSigned());
		assertEquals("utf8mb4", ((StringColumnDef) table.findColumn(2)).getCharset());
		assertEquals("blob", table.findColumn(3).getType());
		assertEquals("binary", ((StringColumnDef) table.findColumn(3)).getCharset());
		assertEquals(Arrays.asList("new", "done"), ((EnumeratedColumnDef) table.findColumn(4)).getEnumValues());
		assertEquals(Arrays.asList("a", "b"), ((EnumeratedColumnDef) table.findColumn(5)).getEnumValues());
		assertEquals(Long.valueOf(6), ((ColumnDefWithLength) table.findColumn(6)).getColumnLength());
		assertEquals("point", table.findColumn(7).getType());
	}

	@Test
	public void supportsDefaultCharsetWithPerColumnOverride() {
		TableMapEventData event = event(
			new ColumnType[] { ColumnType.LONG, ColumnType.VARCHAR, ColumnType.BLOB },
			new int[] { 0, 100, 2 },
			"number_value", "text_value", "binary_value"
		);
		event.getEventMetadata().setSignedness(new BitSet());

		TableMapEventMetadata.DefaultCharset defaultCharset = new TableMapEventMetadata.DefaultCharset();
		defaultCharset.setDefaultCharsetCollation(255);
		Map<Integer, Integer> overrides = new LinkedHashMap<>();
		overrides.put(1, 63);
		defaultCharset.setCharsetCollations(overrides);
		event.getEventMetadata().setDefaultCharset(defaultCharset);

		Table table = BinlogTableMetadata.buildTable(event);
		assertEquals("varchar", table.findColumn(1).getType());
		assertEquals("blob", table.findColumn(2).getType());
	}

	@Test
	public void rejectsTableMapWithoutFullMetadata() {
		TableMapEventData event = new TableMapEventData();
		event.setDatabase("homs");
		event.setTable("orders");
		event.setColumnTypes(new byte[] { (byte) ColumnType.LONG.getCode() });
		event.setColumnMetadata(new int[] { 0 });

		IllegalStateException error = assertThrows(
			IllegalStateException.class,
			() -> BinlogTableMetadata.buildTable(event)
		);
		assertTrue(error.getMessage().contains("binlog_row_metadata=FULL"));
	}

	@Test
	public void tableCacheReplacesDefinitionOnEveryTableMap() {
		TableCache cache = new TableCache("maxwell");
		Filter filter = new Filter();

		TableMapEventData first = integerEvent("old_name");
		TableMapEventData second = integerEvent("new_name");
		cache.processEvent(first, filter);
		cache.processEvent(second, filter);

		assertEquals("new_name", cache.getTable(42L).findColumn(0).getName());
	}

	private static TableMapEventData integerEvent(String columnName) {
		TableMapEventData event = event(new ColumnType[] { ColumnType.LONG }, new int[] { 0 }, columnName);
		event.setTableId(42L);
		event.getEventMetadata().setSignedness(new BitSet());
		return event;
	}

	private static TableMapEventData event(ColumnType[] types, int[] typeMetadata, String... names) {
		TableMapEventData event = new TableMapEventData();
		event.setTableId(42L);
		event.setDatabase("homs");
		event.setTable("orders");
		byte[] typeCodes = new byte[types.length];
		for (int i = 0; i < types.length; i++)
			typeCodes[i] = (byte) types[i].getCode();
		event.setColumnTypes(typeCodes);
		event.setColumnMetadata(typeMetadata);

		TableMapEventMetadata metadata = new TableMapEventMetadata();
		metadata.setColumnNames(Arrays.asList(names));
		event.setEventMetadata(metadata);
		return event;
	}
}

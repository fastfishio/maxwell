package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellTestSupport;
import com.zendesk.maxwell.MysqlIsolatedServer;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

public class BinlogRowMetadataIntegrationTest {
	@Test
	public void followsGhostCutoverWithoutProcessingDDL() throws Exception {
		assumeTrue(MysqlIsolatedServer.getVersion().atLeast(8, 0));
		assumeTrue(!MysqlIsolatedServer.getVersion().isMariaDB);

		MysqlIsolatedServer server = MaxwellTestSupport.setupServer("--binlog-row-metadata=FULL");
		try {
			Filter filter = new Filter(
				"exclude: *.*,include: homs.*,exclude: homs./^_.*_(gho|ghc|del)$/"
			);

			String[] before = {
				"DROP DATABASE IF EXISTS homs",
				"CREATE DATABASE homs",
				"CREATE TABLE homs.order_line (id BIGINT UNSIGNED PRIMARY KEY, routing_approach VARCHAR(32))"
			};
			String[] changes = {
				"INSERT INTO homs.order_line VALUES (1, 'legacy')",
				"CREATE TABLE homs._order_line_gho LIKE homs.order_line",
				"ALTER TABLE homs._order_line_gho MODIFY routing_approach INT NOT NULL",
				"INSERT INTO homs._order_line_gho VALUES (2, 3)",
				"RENAME TABLE homs.order_line TO homs._order_line_del, homs._order_line_gho TO homs.order_line",
				"INSERT INTO homs.order_line VALUES (3, -4)",
				"DROP TABLE homs._order_line_del"
			};

			List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(
				server,
				changes,
				before,
				config -> configureBinlogMetadataMode(config, filter)
			);

			assertEquals(2, rows.size());
			assertEquals("order_line", rows.get(0).getTable());
			assertEquals("legacy", rows.get(0).getData("routing_approach"));
			assertEquals(1L, ((Number) rows.get(0).getData("id")).longValue());
			assertEquals(-4L, ((Number) rows.get(1).getData("routing_approach")).longValue());
			assertEquals(3L, ((Number) rows.get(1).getData("id")).longValue());
			assertFalse(rows.stream().anyMatch(row -> row.getTable().startsWith("_")));

			try (ResultSet schemas = server.query("SELECT COUNT(*) FROM maxwell.schemas")) {
				schemas.next();
				assertEquals(0, schemas.getInt(1));
			}
		} finally {
			server.shutDown();
		}
	}

	private static void configureBinlogMetadataMode(MaxwellConfig config, Filter filter) {
		config.schemaSource = MaxwellConfig.SCHEMA_SOURCE_BINLOG;
		config.filter = filter;
	}
}

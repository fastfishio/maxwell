package com.zendesk.maxwell.schema.columndef;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RawJSONString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonColumnDef extends ColumnDef {
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonColumnDef.class);

	private JsonColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	public static JsonColumnDef create(String name, String type, short pos) {
		JsonColumnDef temp = new JsonColumnDef(name, type, pos);
		return (JsonColumnDef) INTERNER.intern(temp);
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		String jsonString;

		if ( value instanceof String ) {
			return new RawJSONString((String) value);
		} else if ( value instanceof byte[] ){
			try {
				byte[] bytes = (byte[]) value;
				if (bytes.length > 16384) {
					LOGGER.warn("Skipping JSON over limit for field: " + name);
					jsonString = "null";
				} else {
					jsonString = bytes.length > 0 ? JsonBinary.parseAsString(bytes) : "null";
				}
				return new RawJSONString(jsonString);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}

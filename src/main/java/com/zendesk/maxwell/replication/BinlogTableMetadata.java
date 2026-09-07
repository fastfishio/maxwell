package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.mysql.cj.CharsetMapping;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the table definition needed to decode a row directly from a MySQL 8
 * TABLE_MAP event generated with binlog_row_metadata=FULL.
 */
final class BinlogTableMetadata {
	private BinlogTableMetadata() { }

	static Table buildTable(TableMapEventData event) {
		TableMapEventMetadata metadata = event.getEventMetadata();
		byte[] columnTypes = event.getColumnTypes();
		int[] columnMetadata = event.getColumnMetadata();

		if (metadata == null || metadata.getColumnNames() == null) {
			throw metadataError(event, "column names are absent");
		}

		List<String> names = metadata.getColumnNames();
		if (columnTypes == null || columnMetadata == null ||
			columnTypes.length != names.size() || columnMetadata.length != names.size()) {
			throw metadataError(event, "column name, type, and metadata counts do not match");
		}

		List<ColumnDef> columns = new ArrayList<>(names.size());
		int characterIndex = 0;
		int enumIndex = 0;
		int setIndex = 0;
		int geometryIndex = 0;

		for (int i = 0; i < names.size(); i++) {
			ColumnType type = effectiveType(columnTypes[i], columnMetadata[i]);
			String charset = null;
			String[] enumValues = null;

			if (isCharacterType(type)) {
				Integer collation = collationForColumn(
					characterIndex++,
					metadata.getColumnCharsets(),
					metadata.getDefaultCharset()
				);
				charset = charsetForCollation(event, names.get(i), collation);
			} else if (type == ColumnType.ENUM) {
				enumValues = typeValues(event, names.get(i), metadata.getEnumStrValues(), enumIndex++, "ENUM");
			} else if (type == ColumnType.SET) {
				enumValues = typeValues(event, names.get(i), metadata.getSetStrValues(), setIndex++, "SET");
			}

			String sqlType = sqlType(type, columnMetadata[i], charset, metadata, geometryIndex);
			if (type == ColumnType.GEOMETRY)
				geometryIndex++;

			boolean signed = isSigned(event, type, i, metadata.getSignedness());
			Long columnLength = temporalPrecision(type, columnMetadata[i]);
			columns.add(ColumnDef.build(names.get(i), charset, sqlType, (short) i, signed, enumValues, columnLength));
		}

		return new Table(event.getDatabase(), event.getTable(), null, columns, primaryKeys(event, metadata, names));
	}

	private static ColumnType effectiveType(byte rawType, int metadata) {
		ColumnType type = ColumnType.byCode(rawType & 0xff);
		if (type == null)
			throw new IllegalArgumentException("Unsupported binlog column type code " + (rawType & 0xff));

		// MySQL encodes ENUM, SET, and long CHAR variants inside MYSQL_TYPE_STRING metadata.
		if (type == ColumnType.STRING && metadata >= 256) {
			int encodedType = metadata >> 8;
			if ((encodedType & 0x30) != 0x30)
				encodedType |= 0x30;

			ColumnType effective = ColumnType.byCode(encodedType);
			if (effective == ColumnType.ENUM || effective == ColumnType.SET)
				return effective;
		}

		return type;
	}

	private static boolean isCharacterType(ColumnType type) {
		switch (type) {
			case VARCHAR:
			case VAR_STRING:
			case STRING:
			case TINY_BLOB:
			case MEDIUM_BLOB:
			case LONG_BLOB:
			case BLOB:
				return true;
			default:
				return false;
		}
	}

	private static Integer collationForColumn(
		int characterIndex,
		List<Integer> columnCharsets,
		TableMapEventMetadata.DefaultCharset defaultCharset
	) {
		if (columnCharsets != null) {
			if (characterIndex >= columnCharsets.size())
				return null;
			return columnCharsets.get(characterIndex);
		}

		if (defaultCharset == null)
			return null;

		Map<Integer, Integer> overrides = defaultCharset.getCharsetCollations();
		// DEFAULT_CHARSET override indexes are relative to character columns,
		// unlike primary-key and column-name indexes, which address all columns.
		if (overrides != null && overrides.containsKey(characterIndex))
			return overrides.get(characterIndex);

		return defaultCharset.getDefaultCharsetCollation();
	}

	private static String charsetForCollation(TableMapEventData event, String column, Integer collation) {
		if (collation == null)
			throw metadataError(event, "character set metadata is absent for column " + column);

		String charset = CharsetMapping.getStaticMysqlCharsetNameForCollationIndex(collation);
		if (charset == null)
			throw metadataError(event, "unsupported collation " + collation + " for column " + column);
		return charset;
	}

	private static String[] typeValues(
		TableMapEventData event,
		String column,
		List<String[]> values,
		int index,
		String type
	) {
		if (values == null || index >= values.size())
			throw metadataError(event, type + " values are absent for column " + column);
		return values.get(index);
	}

	private static boolean isSigned(TableMapEventData event, ColumnType type, int columnIndex, BitSet unsignedColumns) {
		if (!isIntegerType(type))
			return true;
		if (unsignedColumns == null)
			throw metadataError(event, "signedness metadata is absent for integer column " + columnIndex);

		// MySQL's SIGNEDNESS optional metadata bitmap has a set bit for UNSIGNED columns.
		return !unsignedColumns.get(columnIndex);
	}

	private static boolean isIntegerType(ColumnType type) {
		switch (type) {
			case TINY:
			case SHORT:
			case INT24:
			case LONG:
			case LONGLONG:
				return true;
			default:
				return false;
		}
	}

	private static Long temporalPrecision(ColumnType type, int metadata) {
		switch (type) {
			case TIMESTAMP_V2:
			case DATETIME_V2:
			case TIME_V2:
				return (long) metadata;
			default:
				return null;
		}
	}

	private static String sqlType(
		ColumnType type,
		int metadata,
		String charset,
		TableMapEventMetadata eventMetadata,
		int geometryIndex
	) {
		switch (type) {
			case DECIMAL:
			case NEWDECIMAL:
				return "decimal";
			case TINY:
				return "tinyint";
			case SHORT:
				return "smallint";
			case INT24:
				return "mediumint";
			case LONG:
				return "int";
			case LONGLONG:
				return "bigint";
			case FLOAT:
				return "float";
			case DOUBLE:
				return "double";
			case TIMESTAMP:
			case TIMESTAMP_V2:
				return "timestamp";
			case DATE:
			case NEWDATE:
				return "date";
			case TIME:
			case TIME_V2:
				return "time";
			case DATETIME:
			case DATETIME_V2:
				return "datetime";
			case YEAR:
				return "year";
			case VARCHAR:
			case VAR_STRING:
				return isBinary(charset) ? "varbinary" : "varchar";
			case STRING:
				return isBinary(charset) ? "binary" : "char";
			case BIT:
				return "bit";
			case JSON:
				return "json";
			case ENUM:
				return "enum";
			case SET:
				return "set";
			case TINY_BLOB:
				return isBinary(charset) ? "tinyblob" : "tinytext";
			case MEDIUM_BLOB:
				return isBinary(charset) ? "mediumblob" : "mediumtext";
			case LONG_BLOB:
				return isBinary(charset) ? "longblob" : "longtext";
			case BLOB:
				return blobType(metadata, isBinary(charset));
			case GEOMETRY:
				return geometryType(eventMetadata.getGeometryTypes(), geometryIndex);
			default:
				throw new IllegalArgumentException("Unsupported binlog column type " + type);
		}
	}

	private static boolean isBinary(String charset) {
		return "binary".equalsIgnoreCase(charset);
	}

	private static String blobType(int metadata, boolean binary) {
		String prefix;
		switch (metadata) {
			case 1:
				prefix = "tiny";
				break;
			case 3:
				prefix = "medium";
				break;
			case 4:
				prefix = "long";
				break;
			case 2:
			default:
				prefix = "";
				break;
		}
		return prefix + (binary ? "blob" : "text");
	}

	private static String geometryType(List<Integer> geometryTypes, int index) {
		if (geometryTypes == null || index >= geometryTypes.size())
			return "geometry";

		switch (geometryTypes.get(index)) {
			case 1: return "point";
			case 2: return "linestring";
			case 3: return "polygon";
			case 4: return "multipoint";
			case 5: return "multilinestring";
			case 6: return "multipolygon";
			case 7: return "geometrycollection";
			default: return "geometry";
		}
	}

	private static List<String> primaryKeys(
		TableMapEventData event,
		TableMapEventMetadata metadata,
		List<String> names
	) {
		Set<Integer> indexes = new LinkedHashSet<>();
		if (metadata.getSimplePrimaryKeys() != null)
			indexes.addAll(metadata.getSimplePrimaryKeys());
		if (metadata.getPrimaryKeysWithPrefix() != null)
			indexes.addAll(metadata.getPrimaryKeysWithPrefix().keySet());

		List<String> keys = new ArrayList<>(indexes.size());
		for (Integer index : indexes) {
			if (index == null || index < 0 || index >= names.size())
				throw metadataError(event, "invalid primary-key column index " + index);
			keys.add(names.get(index));
		}
		return keys;
	}

	private static IllegalStateException metadataError(TableMapEventData event, String detail) {
		return new IllegalStateException(
			"TABLE_MAP metadata for " + event.getDatabase() + "." + event.getTable() + " is incomplete: " + detail +
			". Enable binlog_row_metadata=FULL before the starting binlog position or use --schema_source=mysql."
		);
	}
}

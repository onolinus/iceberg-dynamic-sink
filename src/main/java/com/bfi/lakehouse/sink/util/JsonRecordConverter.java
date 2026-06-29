package com.bfi.lakehouse.sink.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Jackson {@link JsonNode} (representing a JSON object) into a
 * Flink {@link RowData} using the provided {@link RowType} for field ordering
 * and type coercion.
 *
 * <h3>Type coercions</h3>
 * <pre>
 *   JSON boolean  → Boolean
 *   JSON number   → Integer / Long / Float / Double (narrowed to the RowType's type)
 *   JSON string   → StringData
 *   JSON null     → null
 *   JSON object   → GenericRowData (recursive)
 *   JSON array    → GenericArrayData
 *   anything else → StringData.fromString(node.asText())
 * </pre>
 *
 * <p>Fields present in the {@link RowType} but absent from the JSON node are
 * mapped to {@code null} (Iceberg will fill them with column defaults or null).
 */
public final class JsonRecordConverter {

    private JsonRecordConverter() {}

    /**
     * Converts a JSON object node to a Flink {@link RowData}.
     *
     * @param node    the JSON object node (must not be null)
     * @param rowType the Flink row type describing expected fields
     * @return a {@link GenericRowData} populated from the JSON fields
     */
    public static RowData toRowData(JsonNode node, RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        GenericRowData row = new GenericRowData(RowKind.INSERT, fieldCount);

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldCount; i++) {
            String fieldName = fieldNames.get(i);
            LogicalType fieldType = rowType.getTypeAt(i);
            JsonNode fieldNode = node.get(fieldName);   // null if field absent in JSON
            row.setField(i, convertValue(fieldNode, fieldType));
        }
        return row;
    }

    // -------------------------------------------------------------------------
    // Internal conversion
    // Date, timestamp and bitmap
    // -------------------------------------------------------------------------

    private static Object convertValue(JsonNode node, LogicalType type) {
        if (node == null || node.isNull()) {
            return null;
        }

        LogicalTypeRoot root = type.getTypeRoot();

        switch (root) {
            case BOOLEAN:
                return node.booleanValue();

            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return node.intValue();

            case BIGINT:
                return node.longValue();

            case FLOAT:
                return (float) node.doubleValue();

            case DOUBLE:
                return node.doubleValue();

            case CHAR:
            case VARCHAR:
                return StringData.fromString(node.asText());

            case BINARY:
            case VARBINARY:
                // JSON strings are treated as UTF-8 bytes for binary columns
                try {
                    return node.asText().getBytes("UTF-8");
                } catch (Exception e) {
                    return node.asText().getBytes();
                }

            case DATE:
                // Expect ISO date string "YYYY-MM-DD" or epoch-days integer
                if (node.isNumber()) return node.intValue();
                return parseDateToEpochDays(node.asText());

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Expect epoch-millis (long) or ISO-8601 string
                if (node.isNumber()) {
                    return TimestampData.fromEpochMillis(node.longValue());
                }
                return TimestampData.fromEpochMillis(
                        java.time.Instant.parse(node.asText()).toEpochMilli());

            case ROW:
                if (node.isObject()) {
                    return toRowData(node, (RowType) type);
                }
                return null;

            case ARRAY: {
                ArrayType arrayType = (ArrayType) type;
                LogicalType elementType = arrayType.getElementType();
                List<Object> elements = new ArrayList<>();
                for (JsonNode elem : node) {
                    elements.add(convertValue(elem, elementType));
                }
                return new GenericArrayData(elements.toArray());
            }

            default:
                // Fallback: represent as string
                return StringData.fromString(node.asText());
        }
    }

    private static int parseDateToEpochDays(String isoDate) {
        return (int) java.time.LocalDate.parse(isoDate)
                .toEpochDay();
    }
}

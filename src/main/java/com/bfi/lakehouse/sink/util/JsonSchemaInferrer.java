package com.bfi.lakehouse.sink.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Infers an Apache Iceberg {@link Schema} by inspecting a {@link JsonNode}.
 *
 * <h3>Type mapping</h3>
 * <pre>
 *   JSON boolean  → Iceberg BooleanType
 *   JSON integer  → Iceberg LongType    (safe for all integral values)
 *   JSON decimal  → Iceberg DoubleType
 *   JSON string   → Iceberg StringType
 *   JSON null     → Iceberg StringType  (widest safe default; evolves naturally)
 *   JSON object   → Iceberg StructType  (nested, recursive)
 *   JSON array    → Iceberg ListType    (element type inferred from first element)
 * </pre>
 *
 * <h3>Schema evolution</h3>
 * {@link #mergeSchemas} merges a newly inferred schema into an existing one by
 * appending any previously unseen fields.  Existing field types are never
 * changed (Iceberg does not support narrowing casts).
 */
public final class JsonSchemaInferrer {

    private JsonSchemaInferrer() {}

    /**
     * Infers an Iceberg {@link Schema} from the top-level fields of a JSON object node.
     *
     * @param node           the root JSON object
     * @param fieldIdCounter monotonically increasing counter used to assign unique field IDs
     */
    public static Schema inferSchema(JsonNode node, AtomicInteger fieldIdCounter) {
        if (!node.isObject()) {
            throw new IllegalArgumentException(
                    "Root JSON node must be an object, got: " + node.getNodeType());
        }
        return new Schema(inferFields(node, fieldIdCounter));
    }

    /**
     * Merges {@code inferred} into {@code existing} by appending fields that are
     * present in {@code inferred} but absent from {@code existing}.
     *
     * <p>Already-known fields are left untouched — their types are not updated
     * even if the incoming JSON implies a different type.
     *
     * @param existing       the cached Iceberg schema for this topic
     * @param inferred       schema freshly inferred from the current message
     * @param fieldIdCounter used to assign IDs to newly discovered fields
     * @return the merged schema (may be identical to {@code existing} if no new fields)
     */
    public static Schema mergeSchemas(
            Schema existing,
            Schema inferred,
            AtomicInteger fieldIdCounter) {

        Set<String> knownNames = new HashSet<>();
        for (Types.NestedField f : existing.columns()) {
            knownNames.add(f.name());
        }

        List<Types.NestedField> merged = new ArrayList<>(existing.columns());
        for (Types.NestedField newField : inferred.columns()) {
            if (!knownNames.contains(newField.name())) {
                int freshId = fieldIdCounter.getAndIncrement();
                merged.add(Types.NestedField.optional(freshId, newField.name(), newField.type()));
            }
        }

        return new Schema(merged);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static List<Types.NestedField> inferFields(
            JsonNode node, AtomicInteger counter) {

        List<Types.NestedField> fields = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            int fieldId = counter.getAndIncrement();
            Type type = inferType(entry.getValue(), counter);
            // All inferred fields are optional — JSON fields can always be absent
            fields.add(Types.NestedField.optional(fieldId, entry.getKey(), type));
        }
        return fields;
    }

    private static Type inferType(JsonNode value, AtomicInteger counter) {
        switch (value.getNodeType()) {
            case BOOLEAN:
                return Types.BooleanType.get();
            case NUMBER:
                if (value.isIntegralNumber()) {
                    return Types.LongType.get();
                }
                return Types.DoubleType.get();
            case OBJECT:
                List<Types.NestedField> nested = inferFields(value, counter);
                return Types.StructType.of(nested);
            case ARRAY:
                if (value.size() > 0) {
                    // Infer element type from the first element only
                    Type elementType = inferType(value.get(0), counter);
                    return Types.ListType.ofOptional(counter.getAndIncrement(), elementType);
                }
                // Empty array: default element type to string
                return Types.ListType.ofOptional(counter.getAndIncrement(), Types.StringType.get());
            case NULL:
            case STRING:
            default:
                return Types.StringType.get();
        }
    }
}

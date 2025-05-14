package com.microsoft.semantickernel.data.jdbc.oracle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreQueryProvider;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordDataField;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordDefinition;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordField;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordKeyField;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordVectorField;
import com.microsoft.semantickernel.data.vectorstorage.options.UpsertRecordOptions;
import com.microsoft.semantickernel.exceptions.SKException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import oracle.jdbc.OracleType;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OracleVectorStoreQueryProvider extends JDBCVectorStoreQueryProvider {

    // This could be removed if super.collectionTable made protected
    private final String collectionsTable;

    // This could be common to all query providers
    private final ObjectMapper objectMapper;

    private static final Object dbCreationLock = new Object();


    public enum StringTypeMapping {
        /**
         * Maps String to CLOB
         */
        USE_CLOB,
        /**
         * Maps String to VARCHAR2(4000)
         */
        USE_VARCHAR
    }

    private OracleVectorStoreQueryProvider(
        @Nonnull DataSource dataSource,
        @Nonnull String collectionsTable,
        @Nonnull String prefixForCollectionTables,
        int defaultVarcharSize,
        @Nonnull StringTypeMapping stringTypeMapping,
        ObjectMapper objectMapper) {
        super(
            dataSource,
            collectionsTable,
            prefixForCollectionTables,
            buildSupportedKeyTypes(),
            buildSupportedDataTypes(stringTypeMapping, defaultVarcharSize),
            buildSupportedVectorTypes(defaultVarcharSize));
        this.collectionsTable = collectionsTable;
        this.objectMapper = objectMapper;
    }

    private static HashMap<Class<?>, String> buildSupportedKeyTypes() {
        HashMap<Class<?>, String> supportedKeyTypes = new HashMap<>();
        supportedKeyTypes.put(String.class, "VARCHAR(255)");
        return supportedKeyTypes;
    }
    private static Map<Class<?>, String> buildSupportedVectorTypes(int defaultVarCharLength) {
        HashMap<Class<?>, String> supportedVectorTypes = new HashMap<>();
        supportedVectorTypes.put(String.class, "VECTOR(%s)");
        supportedVectorTypes.put(List.class, "VECTOR(%s)");
        supportedVectorTypes.put(Collection.class, "VECTOR(%s)");
        return supportedVectorTypes;
    }

    private static Map<Class<?>, String> buildSupportedDataTypes(StringTypeMapping stringTypeMapping, int defaultVarCharLength) {
        HashMap<Class<?>, String> supportedDataTypes = new HashMap<>();
        if (stringTypeMapping.equals(StringTypeMapping.USE_VARCHAR)) {
            supportedDataTypes.put(String.class, "VARCHAR(" + defaultVarCharLength + ")");
        } else {
            supportedDataTypes.put(String.class, "CLOB");
        }
        supportedDataTypes.put(Integer.class, "INTEGER");
        supportedDataTypes.put(int.class, "INTEGER");
        supportedDataTypes.put(Long.class, "LONG");
        supportedDataTypes.put(long.class, "LONG");
        supportedDataTypes.put(Float.class, "REAL");
        supportedDataTypes.put(float.class, "REAL");
        supportedDataTypes.put(Double.class, "DOUBLE");
        supportedDataTypes.put(double.class, "DOUBLE");
        supportedDataTypes.put(Boolean.class, "BOOLEAN");
        supportedDataTypes.put(boolean.class, "BOOLEAN");
        supportedDataTypes.put(OffsetDateTime.class, "TIMESTAMPTZ");
        supportedDataTypes.put(List.class, "JSON");
        return supportedDataTypes;
    }

    private String createIndexForVectorField(String collectionName, VectorStoreRecordVectorField vectorField) {
        switch (vectorField.getIndexKind()) {
            case HNSW:
                // TODO: create IVFFLAT for now
            case IVFFLAT:
                return "CREATE VECTOR INDEX IF NOT EXISTS "
                    + getIndexName(vectorField.getEffectiveStorageName())
                    + " ON "
                    + getCollectionTableName(collectionName) + "( " + vectorField.getEffectiveStorageName() + " ) "
                    + " ORGANIZATION NEIGHBOR PARTITIONS "
                    + " WITH DISTANCE COSINE "
                    + "PARAMETERS ( TYPE IVF )";
            default:
                throw new IllegalArgumentException("Unsupported index kind: " + vectorField.getIndexKind());
        }
    }

    private String getIndexName(String effectiveStorageName) {
        return effectiveStorageName + "_VECTOR_INDEX";
    }


    protected String getVectorColumnNamesAndTypes(List<VectorStoreRecordVectorField> fields,
        Map<Class<?>, String> types) {
        List<String> columns = fields.stream()
            .map(field -> validateSQLidentifier(field.getEffectiveStorageName()) + " "
                + String.format(types.get(field.getFieldType()), field.getDimensions() > 0 ? field.getDimensions() + ", FLOAT32" : "FLOAT32"))
            .collect(Collectors.toList());

        return String.join(", ", columns);
    }

    @Override
    protected String getInsertCollectionQuery(String collectionsTable) {
        return formatQuery(
            "MERGE INTO %s existing "+
                "USING (SELECT ? AS collectionId FROM DUAL) new ON (existing.collectionId = new.collectionId) " +
                "WHEN NOT MATCHED THEN INSERT (existing.collectionId) VALUES (new.collectionId)",
            collectionsTable);
    }

    @Override
    public void createCollection(String collectionName,
        VectorStoreRecordDefinition recordDefinition) {

        synchronized (dbCreationLock) {

            List<VectorStoreRecordVectorField> vectorFields = recordDefinition.getVectorFields();
            String createStorageTable = formatQuery("CREATE TABLE IF NOT EXISTS %s ("
                    + "%s VARCHAR(255) PRIMARY KEY, "
                    + "%s, "
                    + "%s)",
                getCollectionTableName(collectionName),
                getKeyColumnName(recordDefinition.getKeyField()),
                getColumnNamesAndTypes(new ArrayList<>(recordDefinition.getDataFields()),
                    getSupportedDataTypes()),
                getVectorColumnNamesAndTypes(new ArrayList<>(vectorFields),
                    getSupportedVectorTypes()));

            String insertCollectionQuery = this.getInsertCollectionQuery(collectionsTable);

            try (Connection connection = dataSource.getConnection()) {
                connection.createStatement().execute(formatQuery("DROP TABLE IF EXISTS %s", getCollectionTableName(collectionName)));
                connection.setAutoCommit(false);
                try (Statement statement = connection.createStatement()) {
                    // Create table
                    System.out.println(createStorageTable);
                    statement.addBatch(createStorageTable);

                    // Create indexed for vectorFields
                    for (VectorStoreRecordVectorField vectorField : vectorFields) {
                        String createVectorIndex = createIndexForVectorField(collectionName,
                            vectorField);

                        if (createVectorIndex != null) {
                            System.out.println(createVectorIndex);
                            statement.addBatch(createVectorIndex);
                        }
                    }
                    statement.executeBatch();

                    try (PreparedStatement insert = connection.prepareStatement(
                        insertCollectionQuery)) {
                        System.out.println(insertCollectionQuery);
                        insert.setString(1, collectionName);
                        insert.execute();
                    }

                    connection.commit();
                } catch (SQLException e) {
                    connection.rollback();
                    throw new SKException("Failed to create collection", e);
                }
            } catch (SQLException e) {
                throw new SKException("Failed to create collection", e);
            }
        }
    }

    @Override
    public void upsertRecords(String collectionName, List<?> records, VectorStoreRecordDefinition recordDefinition, UpsertRecordOptions options) {

        String upsertQuery = formatQuery("MERGE INTO %s existing "+
                "USING (SELECT %s FROM DUAL) new ON (existing.%s = new.%s) " +
                "WHEN MATCHED THEN UPDATE SET %s " +
                "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
            getCollectionTableName(collectionName),
            getNamedWildcard(recordDefinition.getAllFields()),
            getKeyColumnName(recordDefinition.getKeyField()),
            getKeyColumnName(recordDefinition.getKeyField()),
            getUpdateFieldList(recordDefinition.getKeyField(), recordDefinition.getAllFields(), "existing", "new"),
            getInsertFieldList(recordDefinition.getKeyField(), recordDefinition.getAllFields(), "existing"),
            getInsertFieldList(recordDefinition.getKeyField(), recordDefinition.getAllFields(), "new"));

        System.out.println(upsertQuery);
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(upsertQuery)) {
            for (Object record : records) {
                setUpsertStatementValues(statement, record, recordDefinition.getAllFields());
                statement.addBatch();
            }

            statement.executeBatch();
        } catch (SQLException e) {
            throw new SKException("Failed to upsert records", e);
        }
    }

    private void setUpsertStatementValues(PreparedStatement statement, Object record,
        List<VectorStoreRecordField> fields) {
        JsonNode jsonNode = objectMapper.valueToTree(record);

        for (int i = 0; i < fields.size(); ++i) {
            VectorStoreRecordField field = fields.get(i);
            try {
                JsonNode valueNode = jsonNode.get(field.getEffectiveStorageName());

                if (field instanceof VectorStoreRecordVectorField) {
                    // Convert the vector field to a string
                    if (!field.getFieldType().equals(String.class)) {
                        double[] values = StreamSupport.stream(((ArrayNode)valueNode).spliterator(), false).mapToDouble(d -> d.asDouble()).toArray();
                        statement.setObject(i + 1, values, OracleType.VECTOR_FLOAT64);
                        System.out.println("Set values: " + values);
                        continue;
                    }
                } else if (field instanceof VectorStoreRecordDataField) {
                    // Convert List field to a string
                    if (field.getFieldType().equals(List.class)) {
                        statement.setObject(i + 1, objectMapper.writeValueAsString(valueNode));
                        System.out.println("Set values: " + objectMapper.writeValueAsString(valueNode));
                        continue;
                    }
                }

                statement.setObject(i + 1,
                    objectMapper.convertValue(valueNode, field.getFieldType()));
                System.out.println("Set values: " + objectMapper.convertValue(valueNode, field.getFieldType()));
            } catch (SQLException | JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private String getInsertFieldList(VectorStoreRecordKeyField key, List<VectorStoreRecordField> fields, String alias) {
        return fields.stream().map(f -> alias + "." + f.getEffectiveStorageName())
            .collect(Collectors.joining(", "));
    }

    private String getUpdateFieldList(VectorStoreRecordKeyField key, List<VectorStoreRecordField> fields, String oldAlias, String newAlias) {
        return fields.stream().filter(f -> f != key).map(f -> oldAlias + "." + f.getEffectiveStorageName() + " = " +
            newAlias + "." + f.getEffectiveStorageName())
            .collect(Collectors.joining(", "));

    }


    private String getNamedWildcard(List<VectorStoreRecordField> fields) {
        return fields.stream().map(f -> "? " + f.getEffectiveStorageName())
            .collect(Collectors.joining(", "));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
        extends JDBCVectorStoreQueryProvider.Builder {

        private DataSource dataSource;
        private String collectionsTable = DEFAULT_COLLECTIONS_TABLE;
        private String prefixForCollectionTables = DEFAULT_PREFIX_FOR_COLLECTION_TABLES;
        private ObjectMapper objectMapper = new ObjectMapper();
        private StringTypeMapping stringTypeMapping = StringTypeMapping.USE_VARCHAR;
        private int defaultVarcharSize = 4000;


        @SuppressFBWarnings("EI_EXPOSE_REP2")
        public Builder withDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets the collections table name.
         * @param collectionsTable the collections table name
         * @return the builder
         */
        public Builder withCollectionsTable(String collectionsTable) {
            this.collectionsTable = validateSQLidentifier(collectionsTable);
            return this;
        }

        /**
         * Sets the prefix for collection tables.
         * @param prefixForCollectionTables the prefix for collection tables
         * @return the builder
         */
        public Builder withPrefixForCollectionTables(String prefixForCollectionTables) {
            this.prefixForCollectionTables = validateSQLidentifier(prefixForCollectionTables);
            return this;
        }

        public Builder withObjectMapper(
            ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        /**
         * Sets the desired String type mapping.
         * @param stringTypeMapping the desired String type mapping. The default value is
         *                          {@link StringTypeMapping#USE_VARCHAR}
         * @return the builder
         */
        public Builder withStringTypeMapping (StringTypeMapping stringTypeMapping) {
            this.stringTypeMapping = stringTypeMapping;
            return this;
        }

        /**
         * Sets the default size of the VARHCHAR2 fields.
         * @param defaultVarcharSize the default size of the VARHCHAR2 fields. By default, the size
         *                           is 4000.
         * @return then builder
         */
        public Builder withDefaultVarcharSize (int defaultVarcharSize) {
            this.defaultVarcharSize = defaultVarcharSize;
            return this;
        }

        @Override
        public OracleVectorStoreQueryProvider build() {
            return new OracleVectorStoreQueryProvider(dataSource, collectionsTable,
                prefixForCollectionTables, defaultVarcharSize, stringTypeMapping, objectMapper);
        }
    }
}
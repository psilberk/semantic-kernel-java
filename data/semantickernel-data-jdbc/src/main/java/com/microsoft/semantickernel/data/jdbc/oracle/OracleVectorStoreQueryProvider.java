package com.microsoft.semantickernel.data.jdbc.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreQueryProvider;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordDefinition;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordVectorField;
import com.microsoft.semantickernel.data.vectorstorage.options.UpsertRecordOptions;
import com.microsoft.semantickernel.exceptions.SKException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

public class OracleVectorStoreQueryProvider extends JDBCVectorStoreQueryProvider {

    // This could be removed if super.collectionTable made protected
    private final String collectionsTable;

    // This could be common to all query providers
    private final ObjectMapper objectMapper;

    private static final Object dbCreationLock = new Object();

    private OracleVectorStoreQueryProvider(@Nonnull DataSource dataSource, @Nonnull String collectionsTable, @Nonnull String prefixForCollectionTables,
        ObjectMapper objectMapper) {
        super(dataSource, collectionsTable, prefixForCollectionTables);
        this.collectionsTable = collectionsTable;
        this.objectMapper = objectMapper;

        // Update to Oracle supported types
        supportedDataTypes.put(String.class, "CLOB");
        supportedDataTypes.put(List.class, "CLOB");

        // Update to Oracle supported types
        supportedVectorTypes.put(List.class, "VECTOR(%d, FLOAT64)");
        supportedVectorTypes.put(Collection.class, "VECTOR(%d, FLOAT64)");
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
                + String.format(types.get(field.getFieldType()), field.getDimensions()))
            .collect(Collectors.toList());

        return String.join(", ", columns);
    }

    @Override
    protected String getInsertCollectionQuery(String collectionsTable) {
        return formatQuery(
            "MERGE INTO %s existing "+
                "USING (SELECT ? AS collectionId FROM dual) new ON (existing.collectionId = new.collectionId) " +
                "WHEN NOT MATCHED THEN INSERT ( collectionId ) VALUES ( new.collectionId )",
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

        // TODO look for public void createCollection(String collectionName, VectorStoreRecordDefinition recordDefinition) {

        // TODO Make this a MERGE query

//        String upsertStatemente = formatQuery("""
//            MERGE INTO %s EXIST_REC USING (SELECT ? AS ID) NEW_REC ON (EXIST_REC.%s = NEW_REC.ID)
//            WHEN MATACHED THEN UPDATE SET EXISTING REC
//            """,
//                getCollectionTableName(collectionName),
//                recordDefinition.getKeyField().getName(),
//                getQueryColumnsFromFields(fields),
//                getWildcardString(fields.size()),
//                onDuplicateKeyUpdate);super.upsertRecords(collectionName, records, recordDefinition, options);

        String query = formatQuery("INSERT INTO %s (%s, %s, %s) values (?, ?, ?)",
            getCollectionTableName(collectionName),
            recordDefinition.getAllFields().get(0).getStorageName(),
            recordDefinition.getAllFields().get(1).getStorageName(),
            recordDefinition.getAllFields().get(2).getStorageName());

        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(query)) {
            for (Object record : records) {
                JsonNode jsonNode = objectMapper.valueToTree(record);
                for (int i = 0; i < 3; i++) {
                    statement.setObject(i + 1, jsonNode
                        .get(recordDefinition.getAllFields().get(i).getStorageName()).asText());
                }
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new SKException("Failed to upsert records", e);
        }
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

        @Override
        public OracleVectorStoreQueryProvider build() {
            return new OracleVectorStoreQueryProvider(dataSource, collectionsTable,
                prefixForCollectionTables, objectMapper);
        }
    }
}
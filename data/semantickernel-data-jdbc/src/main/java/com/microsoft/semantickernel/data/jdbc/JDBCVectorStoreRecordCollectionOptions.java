// Copyright (c) Microsoft. All rights reserved.
package com.microsoft.semantickernel.data.jdbc;

import com.microsoft.semantickernel.data.vectorstorage.VectorStoreRecordCollectionOptions;
import com.microsoft.semantickernel.data.vectorstorage.VectorStoreRecordMapper;
import com.microsoft.semantickernel.data.vectorstorage.definition.VectorStoreRecordDefinition;
import com.microsoft.semantickernel.exceptions.SKException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.ResultSet;

import static com.microsoft.semantickernel.data.jdbc.SQLVectorStoreQueryProvider.DEFAULT_COLLECTIONS_TABLE;
import static com.microsoft.semantickernel.data.jdbc.SQLVectorStoreQueryProvider.DEFAULT_PREFIX_FOR_COLLECTION_TABLES;

/**
 * Options for a JDBC vector store record collection.
 * @param <Record> the record type
 */
public class JDBCVectorStoreRecordCollectionOptions<Key, Record>
    implements VectorStoreRecordCollectionOptions<Key, Record> {
    protected final Class<Key> keyClass;
    protected final Class<Record> recordClass;
    protected final VectorStoreRecordMapper<Record, ResultSet> vectorStoreRecordMapper;
    protected final VectorStoreRecordDefinition recordDefinition;
    protected final SQLVectorStoreQueryProvider<Key> queryProvider;
    protected final String collectionsTableName;
    protected final String prefixForCollectionTables;

    protected JDBCVectorStoreRecordCollectionOptions(
        Class<Key> keyClass,
        Class<Record> recordClass,
        VectorStoreRecordDefinition recordDefinition,
        VectorStoreRecordMapper<Record, ResultSet> vectorStoreRecordMapper,
        SQLVectorStoreQueryProvider<Key> queryProvider,
        String collectionsTableName,
        String prefixForCollectionTables) {
        this.keyClass = keyClass;
        this.recordClass = recordClass;
        this.recordDefinition = recordDefinition;
        this.vectorStoreRecordMapper = vectorStoreRecordMapper;
        this.queryProvider = queryProvider;
        this.collectionsTableName = collectionsTableName;
        this.prefixForCollectionTables = prefixForCollectionTables;
    }

    /**
     * Gets the key class.
     *
     * @return the key class
     */
    @Override
        public Class<Key> getKeyClass() {
        return keyClass;
    }

    /**
     * Gets the record class.
     * @return the record class
     */
    public Class<Record> getRecordClass() {
        return recordClass;
    }

    /**
     * Gets the record definition.
     * @return the record definition
     */
    public VectorStoreRecordDefinition getRecordDefinition() {
        return recordDefinition;
    }

    /**
     * Gets the vector store record mapper.
     * @return the vector store record mapper
     */
    public VectorStoreRecordMapper<Record, ResultSet> getVectorStoreRecordMapper() {
        return vectorStoreRecordMapper;
    }

    /**
     * Gets the collections table.
     * @return the collections table
     */
    public String getCollectionsTableName() {
        return collectionsTableName;
    }

    /**
     * Gets the prefix for collection tables.
     * @return the prefix for collection tables
     */
    public String getPrefixForCollectionTables() {
        return prefixForCollectionTables;
    }

    /**
     * Gets the query provider.
     * @return the query provider
     */
    @SuppressFBWarnings("EI_EXPOSE_REP") // DataSource in queryProvider is not exposed
    public SQLVectorStoreQueryProvider<Key> getQueryProvider() {
        return queryProvider;
    }

    public static <Record> Builder<String, Record> builder() {
        Builder<String, Record> builder = new Builder<>();
        builder.keyClass = String.class;
        return builder;
    }

    public static <Key, Record> Builder<Key, Record> genericBuilder() {
        return new Builder<>();
    }

    /**
     * Builder for JDBC vector store record collection options.
     * @param <Key> the key type
     * @param <Record> the record type
     */
    public static class Builder<Key, Record> {
        protected Class<Record> recordClass;
        protected Class<Key> keyClass;
        protected VectorStoreRecordDefinition recordDefinition;
        protected VectorStoreRecordMapper<Record, ResultSet> vectorStoreRecordMapper;
        protected SQLVectorStoreQueryProvider<Key> queryProvider;
        protected String collectionsTableName = DEFAULT_COLLECTIONS_TABLE;
        protected String prefixForCollectionTables = DEFAULT_PREFIX_FOR_COLLECTION_TABLES;

        /**
         * Sets the record class.
         * @param recordClass the record class
         * @return the builder
         */
        public Builder<Key, Record> withRecordClass(Class<Record> recordClass) {
            this.recordClass = recordClass;
            return this;
        }

        /**
         * Sets the key class.
         * @param keyClass the key class
         * @return the builder
         */
        public Builder<Key, Record> withKeyClass(Class<Key> keyClass) {
            this.keyClass = keyClass;
            return this;
        }

        /**
         * Sets the record definition.
         * @param recordDefinition the record definition
         * @return the builder
         */
        public Builder<Key, Record> withRecordDefinition(VectorStoreRecordDefinition recordDefinition) {
            this.recordDefinition = recordDefinition;
            return this;
        }

        /**
         * Sets the vector store record mapper.
         * @param vectorStoreRecordMapper the vector store record mapper
         * @return the builder
         */
        public Builder<Key, Record> withVectorStoreRecordMapper(
            VectorStoreRecordMapper<Record, ResultSet> vectorStoreRecordMapper) {
            this.vectorStoreRecordMapper = vectorStoreRecordMapper;
            return this;
        }

        /**
         * Sets the query provider.
         * @param queryProvider the query provider
         * @return the builder
         */
        @SuppressFBWarnings("EI_EXPOSE_REP2") // DataSource in queryProvider is not exposed
        public Builder<Key, Record> withQueryProvider(SQLVectorStoreQueryProvider<Key> queryProvider) {
            this.queryProvider = queryProvider;
            return this;
        }

        /**
         * Sets the collections table name.
         * @param collectionsTableName the collections table name
         * @return the builder
         */
        public Builder<Key, Record> withCollectionsTableName(String collectionsTableName) {
            this.collectionsTableName = JDBCVectorStoreQueryProvider
                .validateSQLidentifier(collectionsTableName);
            return this;
        }

        /**
         * Sets the prefix for collection tables.
         * @param prefixForCollectionTables the prefix for collection tables
         * @return the builder
         */
        public Builder<Key, Record> withPrefixForCollectionTables(String prefixForCollectionTables) {
            this.prefixForCollectionTables = JDBCVectorStoreQueryProvider
                .validateSQLidentifier(prefixForCollectionTables);
            return this;
        }

        /**
         * Builds the options.
         * @return the options
         */
        public JDBCVectorStoreRecordCollectionOptions<Key, Record> build() {
            if (recordClass == null) {
                throw new SKException("recordClass is required");
            }

            if (keyClass == null) {
                throw new SKException("keyClass is required");
            }

            return new JDBCVectorStoreRecordCollectionOptions<>(
                keyClass,
                recordClass,
                recordDefinition,
                vectorStoreRecordMapper,
                queryProvider,
                collectionsTableName,
                prefixForCollectionTables);
        }
    }
}

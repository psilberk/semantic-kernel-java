// Copyright (c) Microsoft. All rights reserved.
package com.microsoft.semantickernel.data.jdbc;

import com.microsoft.semantickernel.builders.SemanticKernelBuilder;
import com.microsoft.semantickernel.exceptions.SKException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

/**
 * The JDBCVectorStoreRecordCollection class represents a collection of records in a JDBC vector
 * store. It implements the SQLVectorStoreRecordCollection interface and provides methods for
 * managing the collection, such as creating, deleting, and upserting records.
 *
 * @param <Record> the type of the records in the collection
 */
public class JDBCVectorStoreRecordCollection<Record>
    extends JDBCVectorStoreGenericRecordCollection<String, Record> {

    /**
     * Creates a new instance of the {@link JDBCVectorStoreRecordCollection}.
     *
     * @param dataSource     the data source
     * @param collectionName the name of the collection
     * @param options        the options
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2") // DataSource is not exposed
    public JDBCVectorStoreRecordCollection(
        @Nonnull DataSource dataSource,
        @Nonnull String collectionName,
        @Nonnull JDBCVectorStoreRecordCollectionOptions<String, Record> options) {
        super(dataSource, collectionName, options);   
    }

    /**
     * Builder for a JDBCVectorStoreRecordCollection.
     *
     * @param <Record> the type of the records in the collection
     */
    public static class Builder<Record>
        implements SemanticKernelBuilder<JDBCVectorStoreRecordCollection<Record>> {

        private DataSource dataSource;
        private String collectionName;
        private JDBCVectorStoreRecordCollectionOptions<String, Record> options;

        /**
         * Sets the data source.
         *
         * @param dataSource the data source
         * @return the builder
         */
        @SuppressFBWarnings("EI_EXPOSE_REP2") // DataSource is not exposed
        public Builder<Record> withDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return the builder
         */
        public Builder<Record> withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the options.
         *
         * @param options the options
         * @return the builder
         */
        public Builder<Record> withOptions(JDBCVectorStoreRecordCollectionOptions<String, Record> options) {
            this.options = options;
            return this;
        }

        @Override
        public JDBCVectorStoreRecordCollection<Record> build() {
            if (dataSource == null) {
                throw new SKException("dataSource is required");
            }
            if (collectionName == null) {
                throw new SKException("collectionName is required");
            }
            if (options == null) {
                throw new SKException("options is required");
            }

            return new JDBCVectorStoreRecordCollection<>(dataSource, collectionName, options);
        }
    }
}

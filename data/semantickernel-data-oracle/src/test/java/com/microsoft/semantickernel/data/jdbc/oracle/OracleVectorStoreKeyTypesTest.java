/*
 ** Oracle Database Vector Store Connector for Semantic Kernel (Java)
 **
 ** Copyright (c) 2025 Oracle and/or its affiliates. All rights reserved.
 **
 ** The MIT License (MIT)
 **
 ** Permission is hereby granted, free of charge, to any person obtaining a copy
 ** of this software and associated documentation files (the "Software"), to
 ** deal in the Software without restriction, including without limitation the
 ** rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 ** sell copies of the Software, and to permit persons to whom the Software is
 ** furnished to do so, subject to the following conditions:
 **
 ** The above copyright notice and this permission notice shall be included in
 ** all copies or substantial portions of the Software.
 **
 ** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 ** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 ** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 ** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 ** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 ** FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 ** IN THE SOFTWARE.
 */
package com.microsoft.semantickernel.data.jdbc.oracle;

import com.microsoft.semantickernel.data.jdbc.JDBCVectorStore;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreRecordCollectionOptions;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreOptions;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreRecordCollectionOptions;
import com.microsoft.semantickernel.data.jdbc.oracle.data.IntTypeKeyBoxed;
import com.microsoft.semantickernel.data.jdbc.oracle.data.IntTypeKeyUnboxed;
import com.microsoft.semantickernel.data.jdbc.oracle.data.LongTypeKeyBoxed;
import com.microsoft.semantickernel.data.jdbc.oracle.data.LongTypeKeyUnboxed;
import com.microsoft.semantickernel.data.jdbc.oracle.data.ShortTypeKeyBoxed;
import com.microsoft.semantickernel.data.jdbc.oracle.data.ShortTypeKeyUnboxed;
import com.microsoft.semantickernel.data.jdbc.oracle.data.UUIDTypeKey;
import com.microsoft.semantickernel.data.vectorstorage.VectorStoreRecordCollection;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordData;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordKey;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordVector;
import com.microsoft.semantickernel.data.vectorstorage.definition.DistanceFunction;
import com.microsoft.semantickernel.data.vectorstorage.definition.IndexKind;
import com.microsoft.semantickernel.data.vectorstorage.options.GetRecordOptions;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import java.util.UUID;

public class OracleVectorStoreKeyTypesTest extends OracleCommonVectorStoreRecordCollectionTest {


        @Test
        public void shortTypeKeyUnboxedTest() {
                OracleVectorStoreQueryProvider<Short> queryProvider = OracleVectorStoreQueryProvider
                                .<Short>builder()
                                .withDataSource(DATA_SOURCE)
                                .build();

                JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                                .withDataSource(DATA_SOURCE)
                                .withOptions(JDBCVectorStoreOptions.builder()
                                                .withQueryProvider(queryProvider)
                                                .build())
                                .build();

                VectorStoreRecordCollection<Short, ShortTypeKeyUnboxed> collection = vectorStore
                                .getCollection("ShortTypeKeyUnboxed",
                                    JDBCVectorStoreRecordCollectionOptions
                                                                .<Short, ShortTypeKeyUnboxed>genericBuilder()
                                        .withKeyClass(Short.class)
                                                                .withRecordClass(ShortTypeKeyUnboxed.class)
                                                                .build())
                                .createCollectionAsync().block();

                collection.createCollectionAsync().block();

                ShortTypeKeyUnboxed record = new ShortTypeKeyUnboxed();
                record.key = 1;
                record.data = "one";
                record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

                collection.upsertAsync(record, null).block();

                ShortTypeKeyUnboxed results = collection.getAsync((short)1,
                                GetRecordOptions.builder().includeVectors(false).build()).block();

                Assert.assertEquals(record.key, results.key);
                Assert.assertEquals(record.data, results.data);

                collection.deleteAsync((short) 1, null).block();

                ShortTypeKeyUnboxed resultsAfterDelete = collection.getAsync(
                                (short) 1,
                                GetRecordOptions.builder().includeVectors(false).build()).block();

                Assert.assertNull(resultsAfterDelete);
        }

        @Test
        public void shortTypeKeyBoxedTest() {
                OracleVectorStoreQueryProvider<Short> queryProvider = OracleVectorStoreQueryProvider
                                .<Short>builder()
                                .withDataSource(DATA_SOURCE)
                                .build();

                JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                                .withDataSource(DATA_SOURCE)
                                .withOptions(JDBCVectorStoreOptions.<Short>builder()
                                                .withQueryProvider(queryProvider)
                                                .build())
                                .build();

                VectorStoreRecordCollection<Short, ShortTypeKeyBoxed> collection = vectorStore
                                .getCollection("ShortTypeKeyBoxed",
                                                JDBCVectorStoreRecordCollectionOptions
                                                                .<Short, ShortTypeKeyBoxed>genericBuilder()
                                                                .withKeyClass(Short.class)
                                                                .withRecordClass(
                                                                                ShortTypeKeyBoxed.class)
                                                                .build())
                                .createCollectionAsync().block();

                collection.createCollectionAsync().block();

                ShortTypeKeyBoxed record = new ShortTypeKeyBoxed();
                record.key = 1;
                record.data = "one";
                record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

                collection.upsertAsync(record, null).block();

                ShortTypeKeyBoxed results = collection.getAsync((short)1,
                                GetRecordOptions.builder().includeVectors(false).build()).block();

                Assert.assertEquals(record.key, results.key);
                Assert.assertEquals(record.data, results.data);

                collection.deleteAsync((short)1, null).block();

                ShortTypeKeyBoxed resultsAfterDelete = collection.getAsync((short)1,
                                GetRecordOptions.builder().includeVectors(false).build()).block();

                Assert.assertNull(resultsAfterDelete);
        }


    @Test
    public void intTypeKeyUnboxedTest() {
            OracleVectorStoreQueryProvider<Integer> queryProvider = OracleVectorStoreQueryProvider.<Integer>builder()
                            .withDataSource(DATA_SOURCE)
                            .build();

            JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                            .withDataSource(DATA_SOURCE)
                            .withOptions(JDBCVectorStoreOptions.<Integer>builder()
                                            .withQueryProvider(queryProvider)
                                            .build())
                            .build();

            VectorStoreRecordCollection<Integer, IntTypeKeyUnboxed> collection = vectorStore
                            .getCollection("IntTypeKeyUnboxed",
                                JDBCVectorStoreRecordCollectionOptions.<Integer, IntTypeKeyUnboxed>genericBuilder()
                                    .withKeyClass(Integer.class)
                                                            .withRecordClass(IntTypeKeyUnboxed.class)
                                                            .build())
                            .createCollectionAsync().block();

            collection.createCollectionAsync().block();

            IntTypeKeyUnboxed record = new IntTypeKeyUnboxed();
            record.key = 1;
            record.data = "one";
            record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

            collection.upsertAsync(record, null).block();

            IntTypeKeyUnboxed results = collection.getAsync(1,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertEquals(record.key, results.key);
            Assert.assertEquals(record.data, results.data);

            collection.deleteAsync(1, null).block();

            IntTypeKeyUnboxed resultsAfterDelete = collection.getAsync(1,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertNull(resultsAfterDelete);
    }
    
    @Test
    public void intTypeKeyBoxedTest() {
            OracleVectorStoreQueryProvider<Integer> queryProvider = OracleVectorStoreQueryProvider.<Integer>builder()
                            .withDataSource(DATA_SOURCE)
                            .build();

            JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                            .withDataSource(DATA_SOURCE)
                            .withOptions(JDBCVectorStoreOptions.<Integer>builder()
                                            .withQueryProvider(queryProvider)
                                            .build())
                            .build();

            VectorStoreRecordCollection<Integer, IntTypeKeyBoxed> collection = vectorStore
                            .getCollection("IntTypeKeyBoxed",
                                JDBCVectorStoreRecordCollectionOptions.<Integer, IntTypeKeyBoxed>genericBuilder()
                                    .withKeyClass(Integer.class)
                                                            .withRecordClass(
                                                                            IntTypeKeyBoxed.class)
                                                            .build())
                            .createCollectionAsync().block();

            collection.createCollectionAsync().block();

            IntTypeKeyBoxed record = new IntTypeKeyBoxed();
            record.key = 1;
            record.data = "one";
            record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

            collection.upsertAsync(record, null).block();

            IntTypeKeyBoxed results = collection.getAsync(1,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertEquals(record.key, results.key);
            Assert.assertEquals(record.data, results.data);

            collection.deleteAsync(1, null).block();

            IntTypeKeyBoxed resultsAfterDelete = collection.getAsync(1,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertNull(resultsAfterDelete);
    }

    @Test
    public void longTypeKeyUnboxedTest() {
            OracleVectorStoreQueryProvider<Integer> queryProvider = OracleVectorStoreQueryProvider.<Integer>builder()
                            .withDataSource(DATA_SOURCE)
                            .build();

            JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                            .withDataSource(DATA_SOURCE)
                            .withOptions(JDBCVectorStoreOptions.<Long>builder()
                                            .withQueryProvider(queryProvider)
                                            .build())
                            .build();

            VectorStoreRecordCollection<Long, LongTypeKeyUnboxed> collection = vectorStore
                            .getCollection("LongTypeKeyUnboxed",
                                JDBCVectorStoreRecordCollectionOptions.<Long, LongTypeKeyUnboxed>genericBuilder()
                                    .withKeyClass(Long.class)
                                                            .withRecordClass(
                                                                            LongTypeKeyUnboxed.class)
                                                            .build())
                            .createCollectionAsync().block();

            collection.createCollectionAsync().block();

            LongTypeKeyUnboxed record = new LongTypeKeyUnboxed();
            record.key = 1;
            record.data = "one";
            record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

            collection.upsertAsync(record, null).block();

            LongTypeKeyUnboxed results = collection.getAsync(1L,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertEquals(record.key, results.key);
            Assert.assertEquals(record.data, results.data);

            collection.deleteAsync(1L, null).block();

            LongTypeKeyUnboxed resultsAfterDelete = collection.getAsync(1L,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertNull(resultsAfterDelete);
    }

    @Test
    public void longTypeKeyBoxedTest() {
            OracleVectorStoreQueryProvider<Long> queryProvider = OracleVectorStoreQueryProvider.<Long>builder()
                            .withDataSource(DATA_SOURCE)
                            .build();

            JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                            .withDataSource(DATA_SOURCE)
                            .withOptions(JDBCVectorStoreOptions.<Long>builder()
                                            .withQueryProvider(queryProvider)
                                            .build())
                            .build();

            VectorStoreRecordCollection<Long, LongTypeKeyBoxed> collection = vectorStore
                            .getCollection("LongTypeKeyBoxed",
                                JDBCVectorStoreRecordCollectionOptions.<Long, LongTypeKeyBoxed>genericBuilder()
                                    .withKeyClass(Long.class)
                                                            .withRecordClass(
                                                                            LongTypeKeyBoxed.class)
                                                            .build())
                            .createCollectionAsync().block();

            collection.createCollectionAsync().block();

            LongTypeKeyBoxed record = new LongTypeKeyBoxed();
            record.key = 1L;
            record.data = "one";
            record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

            collection.upsertAsync(record, null).block();

            LongTypeKeyBoxed results = collection.getAsync(1L,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertEquals(record.key, results.key);
            Assert.assertEquals(record.data, results.data);

            collection.deleteAsync(1L, null).block();

            LongTypeKeyBoxed resultsAfterDelete = collection.getAsync(1L,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertNull(resultsAfterDelete);
    }

    @Test
    public void uuidTypeKeyTest() {
            OracleVectorStoreQueryProvider<UUID> queryProvider = OracleVectorStoreQueryProvider.<UUID>builder()
                            .withDataSource(DATA_SOURCE)
                            .build();

            JDBCVectorStore vectorStore = JDBCVectorStore.builder()
                            .withDataSource(DATA_SOURCE)
                            .withOptions(JDBCVectorStoreOptions.<UUID>builder()
                                            .withQueryProvider(queryProvider)
                                            .build())
                            .build();

            VectorStoreRecordCollection<UUID, UUIDTypeKey> collection = vectorStore
                            .getCollection("UUIDTypeKey",
                                JDBCVectorStoreRecordCollectionOptions.<UUID, UUIDTypeKey>genericBuilder()
                                    .withKeyClass(UUID.class)
                                                            .withRecordClass(
                                                                            UUIDTypeKey.class)
                                                            .build())
                            .createCollectionAsync().block();

            collection.createCollectionAsync().block();

            UUIDTypeKey record = new UUIDTypeKey();
            record.key = UUID.randomUUID();
            record.data = "one";
            record.vector = new float[] { 0.5f, 3.2f, 7.1f, -4.0f, 2.8f, 10.0f, -1.3f, 5.5f };

            collection.upsertAsync(record, null).block();

            UUIDTypeKey results = collection.getAsync(record.key,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertEquals(record.key, results.key);
            Assert.assertEquals(record.data, results.data);

            collection.deleteAsync(record.key, null).block();

            UUIDTypeKey resultsAfterDelete = collection.getAsync(record.key,
                            GetRecordOptions.builder().includeVectors(false).build()).block();

            Assert.assertNull(resultsAfterDelete);
    }

}

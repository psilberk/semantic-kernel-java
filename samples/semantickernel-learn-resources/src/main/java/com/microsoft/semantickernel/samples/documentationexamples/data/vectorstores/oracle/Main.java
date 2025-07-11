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
package com.microsoft.semantickernel.samples.documentationexamples.data.vectorstores.oracle;

import com.microsoft.semantickernel.data.jdbc.JDBCVectorStore;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreOptions;
import com.microsoft.semantickernel.data.jdbc.JDBCVectorStoreRecordCollectionOptions;
import com.microsoft.semantickernel.data.jdbc.oracle.OracleVectorStoreQueryProvider;
import com.microsoft.semantickernel.data.vectorsearch.VectorSearchResults;
import com.microsoft.semantickernel.data.vectorstorage.VectorStoreRecordCollection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import com.microsoft.semantickernel.data.vectorstorage.options.VectorSearchOptions;
import oracle.jdbc.datasource.impl.OracleDataSource;
import reactor.core.publisher.Mono;

public class Main {
    public static void main(String[] args) throws SQLException {

        // Configure the data source
        OracleDataSource dataSource = new OracleDataSource();
        dataSource.setURL("jdbc:oracle:thin:@localhost:1521/FREEPDB1");
        dataSource.setUser("scott");
        dataSource.setPassword("tiger");

        // Build a query provider
        OracleVectorStoreQueryProvider queryProvider = OracleVectorStoreQueryProvider.builder()
            .withDataSource(dataSource)
            .build();

        // Build a vector store
        JDBCVectorStore vectorStore = JDBCVectorStore.builder()
            .withDataSource(dataSource)
            .withOptions(JDBCVectorStoreOptions.builder()
                .withQueryProvider(queryProvider)
                .build())
            .build();

        VectorStoreRecordCollection<String, Book> collection = vectorStore.getCollection(
            "books",
            JDBCVectorStoreRecordCollectionOptions.<Book>builder()
                .withRecordClass(Book.class)
                .build());

        // Create the collection if it doesn't exist yet.
        collection.createCollectionIfNotExistsAsync().block();

        collection.upsertBatchAsync(books, null).block();

        // Retrieve the upserted record.
        Book retrievedBook = collection.getAsync("2", null).block();

        System.out.println(retrievedBook.getAuthor());

        // Generate a vector for your search text, using your chosen embedding generation implementation.
        // Just showing a placeholder method here for brevity.
        List<Float> searchVector = generateEmbeddingsAsync(
            "I'm looking for a horror book.").block();

        // Do the search.
        VectorSearchResults<Book> searchResult = collection.searchAsync(
            searchVector, VectorSearchOptions.builder().withTop(1).build()).block();

        retrievedBook = searchResult.getResults().get(0).getRecord();
        System.out.println("Found Book: " + retrievedBook.getIsbn());

    }

    static List<Book> books = Arrays.asList(
        new Book("1", "one", "sking", 0, null, "horror", List.of(1f, 1f)),
        new Book("2", "two", "squeen", 0, null, "non-fiction", List.of(-1f, -1f)));

    private static Mono<List<Float>> generateEmbeddingsAsync(String text) {
        return Mono.just(List.of(-0.1f, -0.1f));
    }

}
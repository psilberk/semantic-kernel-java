package com.microsoft.semantickernel.data.jdbc.oracle.data;

import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordData;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordKey;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordVector;
import com.microsoft.semantickernel.data.vectorstorage.definition.DistanceFunction;
import com.microsoft.semantickernel.data.vectorstorage.definition.IndexKind;

public class LongTypeKeyUnboxed {
    @VectorStoreRecordKey
    public long key;
    @VectorStoreRecordData(isFilterable = true)
    public String data;
    @VectorStoreRecordVector(dimensions = 8, distanceFunction = DistanceFunction.COSINE_DISTANCE, indexKind = IndexKind.IVFFLAT)
    public float[] vector;

    public LongTypeKeyUnboxed () {}

    public LongTypeKeyUnboxed (long key, String data, float[] vector) {
        this.key = key;
        this.data = data;
        this.vector = vector;
    }
}

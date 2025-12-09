package com.microsoft.semantickernel.data.jdbc.oracle.data;

import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordData;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordKey;
import com.microsoft.semantickernel.data.vectorstorage.annotations.VectorStoreRecordVector;
import com.microsoft.semantickernel.data.vectorstorage.definition.DistanceFunction;
import com.microsoft.semantickernel.data.vectorstorage.definition.IndexKind;
import java.util.UUID;

public class UUIDTypeKey {
    @VectorStoreRecordKey
    public UUID key;
    @VectorStoreRecordData(isFilterable = true)
    public String data;
    @VectorStoreRecordVector(dimensions = 8, distanceFunction = DistanceFunction.COSINE_DISTANCE, indexKind = IndexKind.IVFFLAT)
    public float[] vector;

    public UUIDTypeKey () {}

    public UUIDTypeKey (UUID key, String data, float[] vector) {
        this.key = key;
        this.data = data;
        this.vector = vector;
    }
}

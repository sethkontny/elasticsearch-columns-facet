package org.elasticsearch.search.facet.columns;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldDataType;

import java.util.ArrayList;
import java.util.List;

abstract class ColumnsProc implements FieldData.StringValueInDocProc {

    static ColumnsProc getColumnProc(FieldDataType valueDataType) {
        if (valueDataType instanceof StringFieldDataType) {
            return new NonNumericColumnsProc();

        } else {
            return new NumericColumnsProc();
        }
    }

    final ExtTLongObjectHashMap<InternalFullColumnsFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();


    List<FieldData> keyFieldsData = new ArrayList<FieldData>();

    abstract void setFieldData(FieldData fieldData);


    abstract public void aggregateFun(int docId, InternalFullColumnsFacet.FullEntry entry);

    static int compare(Comparable o1, Comparable o2) {
        if (o1 == null) {
            if (o2 == null) {
                return 0;
            }
            return 1;
        }
        if (o2 == null) {
            return -1;
        }
        return o1.compareTo(o2);
    }

    @Override
    public void onValue(int docId, String value) {

        StringBuilder keyBuf = new StringBuilder();
        String[] keys = new String[keyFieldsData.size()];
        int idx = 0;
        for (FieldData keyFieldData : keyFieldsData) {
            keys[idx] = keyFieldData.docFieldData(docId).getStringValue();
            keyBuf.append(keys[idx]);
            keyBuf.append(",");
            idx++;
        }

        long bucket = keyBuf.toString().hashCode();

        InternalFullColumnsFacet.FullEntry entry = entries.get(bucket);
        if (entry == null) {
            entry = new InternalFullColumnsFacet.FullEntry(keys, bucket, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
            entries.put(bucket, entry);
        }
        entry.count++;
        aggregateFun(docId, entry);
    }

    public void onMissing(int docId) {

    }
}
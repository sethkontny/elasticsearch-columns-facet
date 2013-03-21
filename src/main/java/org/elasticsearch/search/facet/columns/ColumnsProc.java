package org.elasticsearch.search.facet.columns;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.ArrayList;
import java.util.List;

abstract class ColumnsProc implements FieldData.StringValueInDocProc {

    static ColumnsProc getColumnProc(List<FieldMapper> keyFieldsMapper, FieldDataType valueDataType) {

        if (valueDataType instanceof StringFieldDataType) {
            return new NonNumericColumnsProc(keyFieldsMapper);

        } else {
            return new NumericColumnsProc(keyFieldsMapper);
        }
    }

    // Maximum distinct group allowered per shard before a shard throw exception.
    // 0 or clear means no limit.
    // Usage:
    // in config/elasticsearch.yml
    // columns_facet.max_distinct_groups = 10000
    static int MaxDistinctGroups = Integer.parseInt(Config.get("columns_facet.max_distinct_groups", "0"));

    final ExtTLongObjectHashMap<InternalFullColumnsFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();


    List<FieldData> keyFieldsData = new ArrayList<FieldData>();

    List<FieldMapper> keyFieldsMapper;

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
            FieldMapper mapper = keyFieldsMapper.get(idx);
            ComparableConverter conv = ComparableConverter.converters.get(mapper.getClass());
            keys[idx] = conv.getStringValue(docId, keyFieldData);
            keyBuf.append(keys[idx]);
            keyBuf.append(",");
            idx++;
        }

        long bucket = keyBuf.toString().hashCode();

        InternalFullColumnsFacet.FullEntry entry = entries.get(bucket);
        if (entry == null) {
            entry = new InternalFullColumnsFacet.FullEntry(keys, bucket, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
            entries.put(bucket, entry);
            if (MaxDistinctGroups > 0 && entries.size() > MaxDistinctGroups) {
                throw new TooManyDistinctValuesException(MaxDistinctGroups);
            }
        }
        entry.count++;
        aggregateFun(docId, entry);
    }

    public void onMissing(int docId) {

    }
}


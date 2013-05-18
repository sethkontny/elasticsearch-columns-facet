package org.elasticsearch.search.facet.columns;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTHashMap;
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

    final ExtTHashMap<String, InternalFullColumnsFacet.FullEntry> entries = CacheRecycler.popHashMap();


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
        String[] keys = new String[keyFieldsMapper.size()];
        String key;
        int idx = 0;
        for (FieldData keyFieldData : keyFieldsData) {
            FieldMapper mapper = keyFieldsMapper.get(idx);
            ComparableConverter conv = ComparableConverter.converters.get(mapper.getClass());
            key = conv.getStringValue(docId, keyFieldData);
            keys[idx] = key;
            keyBuf.append(key);
            keyBuf.append(',');
            idx++;
        }

        key = keyBuf.toString();
        InternalFullColumnsFacet.FullEntry entry =  entries.get(key);
        if (entry == null) {
            entry = new InternalFullColumnsFacet.FullEntry(keys, key, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
            entries.put(key, entry);
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


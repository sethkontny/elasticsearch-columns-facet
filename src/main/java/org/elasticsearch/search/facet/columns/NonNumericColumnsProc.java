package org.elasticsearch.search.facet.columns;

import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.List;

class NonNumericColumnsProc extends ColumnsProc {

    public NonNumericColumnsProc(List<FieldMapper> keyFieldsMapper) {
        this.keyFieldsMapper = keyFieldsMapper;
    }

    FieldData valueFieldData;

    ValueAggregator valueAggregator = new ValueAggregator();

    public void setFieldData(FieldData fieldData) {
        this.valueFieldData = fieldData;
    }

    public void aggregateFun(int docId, InternalFullColumnsFacet.FullEntry entry) {

        valueAggregator.entry = entry;
        valueFieldData.forEachValueInDoc(docId, valueAggregator);
    }

    private static class ValueAggregator implements FieldData.StringValueInDocProc {

        InternalFullColumnsFacet.FullEntry entry;

        @Override
        public void onValue(int docId, String value) {
            entry.totalCount++;
        }

        public void onMissing(int docId) {

        }
    }
}
package org.elasticsearch.search.facet.columns;

import org.elasticsearch.index.field.data.FieldData;

class NonNumericColumnsProc extends ColumnsProc {

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
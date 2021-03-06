package org.elasticsearch.search.facet.columns;

import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.List;


class NumericColumnsProc extends ColumnsProc {

    public NumericColumnsProc(List<FieldMapper> keyFieldsMapper) {
        this.keyFieldsMapper = keyFieldsMapper;
    }

    NumericFieldData valueFieldData;

    ValueAggregator valueAggregator = new ValueAggregator();

    public void setFieldData(FieldData fieldData) {
        this.valueFieldData = (NumericFieldData)fieldData;
    }

    public void aggregateFun(int docId, InternalFullColumnsFacet.FullEntry entry) {

        valueAggregator.entry = entry;
        valueFieldData.forEachValueInDoc(docId, valueAggregator);
    }

    private static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

        InternalFullColumnsFacet.FullEntry entry;

        @Override
        public void onValue(int docId, double value) {
            entry.totalCount++;
            entry.total += value;
            if (value < entry.min) {
                entry.min = value;
            }
            if (value > entry.max) {
                entry.max = value;
            }
        }
    }
}
package org.elasticsearch.search.facet.columns;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A columns facet collector that uses different fields for the key and the value.
 */
public class ColumnsFacetCollector extends AbstractFacetCollector {


    private final String valueIndexFieldName;


    private final ColumnsFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType valueFieldDataType;

    private final List<String> keyFieldNames;
    private List<FieldData> keyFieldsData;
    private final Map<String, FieldDataType> keyFieldDataTypes;

    private final int keySize;

    private final ColumnsProc columnsProc;

    public ColumnsFacetCollector(String facetName, List<String> keyFieldNames, String valueFieldName, ColumnsFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();
        this.keyFieldNames = keyFieldNames;
        this.keyFieldDataTypes = new HashMap<String, FieldDataType>();

        MapperService.SmartNameFieldMappers smartMappers = null;
        keySize = keyFieldNames.size();
        for (String keyField: keyFieldNames) {
            smartMappers = context.smartFieldMappers(keyField);
            if (smartMappers == null || !smartMappers.hasMapper()) {
                throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + keyField + "]");
            }
            keyFieldDataTypes.put(keyField, smartMappers.mapper().fieldDataType());
            // add type filter if there is exact doc mapper associated with it
            if (smartMappers.explicitTypeInNameWithDocMapper()) {
                setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
            }
        }


        smartMappers = context.smartFieldMappers(valueFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for value_field [" + valueFieldName + "]");
        }
        valueIndexFieldName = smartMappers.mapper().names().indexName();
        valueFieldDataType = smartMappers.mapper().fieldDataType();

        columnsProc = new ColumnsProc();
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        //keyFieldData.forEachValueInDoc(doc, histoProc);
        keyFieldsData.get(0).forEachValueInDoc(doc, columnsProc);
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        columnsProc.valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, valueIndexFieldName);

        keyFieldsData = new ArrayList<FieldData>();
        for (String keyFieldName : keyFieldNames) {
            keyFieldsData.add(fieldDataCache.cache(keyFieldDataTypes.get(keyFieldName), reader, keyFieldName));
        }
        columnsProc.keyFieldsData = keyFieldsData;
    }

    @Override
    public Facet facet() {
        return new InternalFullColumnsFacet(facetName, comparatorType, columnsProc.entries, true);
    }



    public static class ColumnsProc implements FieldData.StringValueInDocProc {

        final ExtTLongObjectHashMap<InternalFullColumnsFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

        NumericFieldData valueFieldData;

        List<FieldData> keyFieldsData = new ArrayList<FieldData>();

        final ValueAggregator valueAggregator = new ValueAggregator();

        public ColumnsProc() {
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

            long bucket = keyBuf.toString().hashCode(); //value.hashCode(); // MyFullHistogramFacetCollector.bucket(value, interval);


            String msg = "onValue: " + value + ", docId: " + Integer.toString(docId) + ", everything: " +
                    keyBuf.toString();
            System.out.println(msg);


            InternalFullColumnsFacet.FullEntry entry = entries.get(bucket);
            if (entry == null) {
                entry = new InternalFullColumnsFacet.FullEntry(keys, bucket, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
                entries.put(bucket, entry);
            }
            entry.count++;
            valueAggregator.entry = entry;
            valueFieldData.forEachValueInDoc(docId, valueAggregator);
        }

        public void onMissing(int docId) {

        }

        public static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

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
                System.out.println("xx total: " + Double.toString(entry.total()) + ":" + Long.toString(entry.totalCount())
                        + ":" + Double.toString(entry.min()) + ":" + Double.toString(entry.max()));
            }
        }
    }
}
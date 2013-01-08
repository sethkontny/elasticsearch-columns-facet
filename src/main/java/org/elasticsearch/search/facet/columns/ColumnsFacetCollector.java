package org.elasticsearch.search.facet.columns;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
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

        columnsProc = ColumnsProc.getColumnProc(valueFieldDataType);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        keyFieldsData.get(0).forEachValueInDoc(doc, columnsProc);
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        columnsProc.setFieldData(fieldDataCache.cache(valueFieldDataType, reader, valueIndexFieldName));
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
}
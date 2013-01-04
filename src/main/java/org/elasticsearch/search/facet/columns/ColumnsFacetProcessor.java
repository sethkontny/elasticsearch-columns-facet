package org.elasticsearch.search.facet.columns;

import com.google.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ColumnsFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject
    public ColumnsFacetProcessor(Settings settings) {
        super(settings);
        InternalColumnsFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[]{ColumnsFacet.TYPE};
    }

    /**
     * TODO: Support keyScript, valueScript, scriptLang, params
     * @param facetName
     * @param parser
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        List<String> keyFields = Lists.newArrayListWithCapacity(4);
        String valueField = null;
        String keyScript = null;
        String valueScript = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        ColumnsFacet.ComparatorType comparatorType = ColumnsFacet.ComparatorType.KEY;
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(fieldName)) {
                    params = parser.map();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("key_fields".equalsIgnoreCase(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        keyFields.add(parser.text());
                    }
                } else if ("orders".equalsIgnoreCase(fieldName)) {
                    List<String> orders = Lists.newArrayListWithCapacity(4);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        orders.add(parser.text());
                    }
                    comparatorType = ColumnsFacet.MultiFieldsComparator.generateComparator(
                            keyFields.toArray(new String[0]), orders.toArray(new String[0]));
                }
            } else if (token.isValue()) {
                if ("value_field".equals(fieldName) || "valueField".equals(fieldName)) {
                    valueField = parser.text();
                } else if ("key_script".equals(fieldName) || "keyScript".equals(fieldName)) {
                    keyScript = parser.text();
                } else if ("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                } else if ("order".equals(fieldName) || "comparator".equals(fieldName)) {
                    comparatorType = ColumnsFacet.ComparatorType.fromString(parser.text());
                } else if ("lang".equals(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (keyFields.isEmpty()) {
            throw new FacetPhaseExecutionException(facetName, "Key fields are required to be set for columns facet.");
        }

        return new ColumnsFacetCollector(facetName, keyFields, valueField, comparatorType, context);
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        InternalColumnsFacet first = (InternalColumnsFacet) facets.get(0);
        return first.reduce(name, facets);
    }
}

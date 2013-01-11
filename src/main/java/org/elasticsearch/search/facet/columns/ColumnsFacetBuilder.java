package org.elasticsearch.search.facet.columns;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.AbstractFacetBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A facet builder of columns facets.
 *
 *
 */
public class ColumnsFacetBuilder extends AbstractFacetBuilder {
    private String keyFieldName;
    private String valueFieldName;
    private long interval = -1;
    private ColumnsFacet.ComparatorType comparatorType;
    private Object from;
    private Object to;

    /**
     * Constructs a new columns facet with the provided facet logical name.
     *
     * @param name The logical name of the facet
     */
    public ColumnsFacetBuilder(String name) {
        super(name);
    }

    /**
     * The field name to perform the columns facet. Translates to perform the columns facet
     * using the provided field as both the {@link #keyField(String)} and {@link #valueField(String)}.
     */
    public ColumnsFacetBuilder field(String field) {
        this.keyFieldName = field;
        return this;
    }

    /**
     * The field name to use in order to control where the hit will "fall into" within the columns
     * entries. Essentially, using the key field numeric value, the hit will be "rounded" into the relevant
     * bucket controlled by the interval.
     */
    public ColumnsFacetBuilder keyField(String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    /**
     * The field name to use as the value of the hit to compute data based on values within the interval
     * (for example, total).
     */
    public ColumnsFacetBuilder valueField(String valueField) {
        this.valueFieldName = valueField;
        return this;
    }

    /**
     * The interval used to control the bucket "size" where each key value of a hit will fall into.
     */
    public ColumnsFacetBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    /**
     * The interval used to control the bucket "size" where each key value of a hit will fall into.
     */
    public ColumnsFacetBuilder interval(long interval, TimeUnit unit) {
        return interval(unit.toMillis(interval));
    }

    /**
     * Sets the bounds from and to for the facet. Both performs bounds check and includes only
     * values within the bounds, and improves performance.
     */
    public ColumnsFacetBuilder bounds(Object from, Object to) {
        this.from = from;
        this.to = to;
        return this;
    }

    public ColumnsFacetBuilder comparator(ColumnsFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    /**
     * Should the facet run in global mode (not bounded by the search query) or not (bounded by
     * the search query). Defaults to <tt>false</tt>.
     */
    public ColumnsFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    /**
     * Marks the facet to run in a specific scope.
     */
    @Override
    public ColumnsFacetBuilder scope(String scope) {
        super.scope(scope);
        return this;
    }

    /**
     * An additional filter used to further filter down the set of documents the facet will run on.
     */
    public ColumnsFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public ColumnsFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyFieldName == null) {
            throw new SearchSourceBuilderException("field must be set on columns facet for facet [" + name + "]");
        }
        if (interval < 0) {
            throw new SearchSourceBuilderException("interval must be set on columns facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(ColumnsFacet.TYPE);
        if (valueFieldName != null) {
            builder.field("key_field", keyFieldName);
            builder.field("value_field", valueFieldName);
        } else {
            builder.field("field", keyFieldName);
        }
        builder.field("interval", interval);

        if (from != null && to != null) {
            builder.field("from", from);
            builder.field("to", to);
        }

        if (comparatorType != null) {
            builder.field("comparator", comparatorType.description());
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }
}

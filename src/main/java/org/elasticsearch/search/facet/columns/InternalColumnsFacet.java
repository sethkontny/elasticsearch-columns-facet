package org.elasticsearch.search.facet.columns;

import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.util.List;

public abstract class InternalColumnsFacet implements ColumnsFacet, InternalFacet {

    public static void registerStreams() {
        InternalFullColumnsFacet.registerStreams();
    }

    public abstract Facet reduce(String name, List<Facet> facets);
}

package org.elasticsearch.plugin.search.facet.columns;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;
import org.elasticsearch.search.facet.columns.ColumnsFacetProcessor;

public class ColumnsFacetPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "facet-columns";
    }

    @Override
    public String description() {
        return "Multiple columns facet support";
    }

    public void onModule(FacetModule facetModule) {
        facetModule.addFacetProcessor(ColumnsFacetProcessor.class);
    }

}

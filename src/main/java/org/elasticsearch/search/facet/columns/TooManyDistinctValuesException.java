package org.elasticsearch.search.facet.columns;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.rest.RestStatus;

public class TooManyDistinctValuesException extends ElasticSearchException {

    public TooManyDistinctValuesException(int maxDistinctGroup) {
        super(String.format("Facet distinct values exceeds max limit - %d", maxDistinctGroup));
    }

    @Override
    public RestStatus status() {
        return RestStatus.FORBIDDEN;
    }
}
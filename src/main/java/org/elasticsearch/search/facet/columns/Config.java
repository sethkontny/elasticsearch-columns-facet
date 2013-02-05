package org.elasticsearch.search.facet.columns;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;

import java.net.URL;

public class Config {

    private static URL url = new Environment().resolveConfig("elasticsearch.yml");

    private static ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().loadFromUrl(url);

    static String get(String key, String valueIfMissing) {
        String v = builder.get(key);
        return (v == null || v.isEmpty()) ? valueIfMissing : v;
    }
}

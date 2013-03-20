package org.elasticsearch.search.facet.columns;

import org.elasticsearch.index.field.data.FieldDataType;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Grouping column values are all stored as string.
 * Help restoring these strings to specific type for ordering.
 * When there are multiple-shards, they need to be serializable for facet reduction.
 */
interface ComparableConverter<T> extends Serializable {
    Comparable<T> toComparable(String s);
    static final long serialVersionUID = 0L;

    static final ComparableConverter<String> stringConv = new ComparableConverter<String>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<String> toComparable(String s) {
            return s;
        }
    };

    static final ComparableConverter<Byte> byteConv = new ComparableConverter<Byte>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Byte> toComparable(String s) { return new Byte(s); }
    };

    static final ComparableConverter<Short> shortConv = new ComparableConverter<Short>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Short> toComparable(String s) { return new Short(s); }
    };

    static final ComparableConverter<Integer> intConv = new ComparableConverter<Integer>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Integer> toComparable(String s) { return new Integer(s); }
    };

    static final ComparableConverter<Long> longConv = new ComparableConverter<Long>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Long> toComparable(String s) { return new Long(s); }
    };

    static final ComparableConverter<Float> floatConv = new ComparableConverter<Float>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Float> toComparable(String s) { return new Float(s); }
    };

    static final ComparableConverter<Double> doubleConv = new ComparableConverter<Double>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Double> toComparable(String s) { return new Double(s); }
    };

    static final Map<FieldDataType, ComparableConverter> converters =
        Collections.unmodifiableMap(new HashMap<FieldDataType, ComparableConverter>() {{
            put(FieldDataType.DefaultTypes.STRING, stringConv);
            put(FieldDataType.DefaultTypes.BYTE, byteConv);
            put(FieldDataType.DefaultTypes.SHORT, shortConv);
            put(FieldDataType.DefaultTypes.INT, intConv);
            put(FieldDataType.DefaultTypes.LONG, longConv);
            put(FieldDataType.DefaultTypes.FLOAT, floatConv);
            put(FieldDataType.DefaultTypes.DOUBLE, doubleConv);
    }});
}

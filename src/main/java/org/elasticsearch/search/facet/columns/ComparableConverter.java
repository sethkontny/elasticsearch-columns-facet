package org.elasticsearch.search.facet.columns;

import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.bytes.ByteFieldData;
import org.elasticsearch.index.field.data.doubles.DoubleFieldData;
import org.elasticsearch.index.field.data.floats.FloatFieldData;
import org.elasticsearch.index.field.data.ints.IntFieldData;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.field.data.shorts.ShortFieldData;
import org.elasticsearch.index.mapper.core.*;

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

    static final long serialVersionUID = 1L;

    Comparable<T> toComparable(String s);

    /**
     * There is no polymorphic function in FieldData to getValue.
     * It only has polymorphic function to operate onValue which does not help when
     * we need to process data from more than one column.
     */
    String getStringValue (int docId, FieldData fieldData);

    static final ComparableConverter<String> stringConv = new ComparableConverter<String>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<String> toComparable(String s) {
            return s;
        }

        public String getStringValue (int docId, FieldData fieldData) {
            return fieldData.stringValue(docId);
        }
    };

    static final ComparableConverter<Byte> byteConv = new ComparableConverter<Byte>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Byte> toComparable(String s) { return new Byte(s); }

        public String getStringValue (int docId, FieldData fieldData) {
            return Byte.toString(((ByteFieldData) fieldData).byteValue(docId));
        }
    };

    static final ComparableConverter<Short> shortConv = new ComparableConverter<Short>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Short> toComparable(String s) { return new Short(s); }

        public String getStringValue (int docId, FieldData fieldData) {
            return Short.toString(((ShortFieldData)fieldData).shortValue(docId));
        }
    };

    static final ComparableConverter<Integer> intConv = new ComparableConverter<Integer>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Integer> toComparable(String s) { return new Integer(s); }

        public String getStringValue (int docId, FieldData fieldData) {
            return Integer.toString(((IntFieldData)fieldData).intValue(docId));
        }
    };

    static final ComparableConverter<Long> longConv = new ComparableConverter<Long>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Long> toComparable(String s) { return new Long(s); }

        public String getStringValue (int docId, FieldData fieldData) {
           return Long.toString(((LongFieldData)fieldData).longValue(docId));
        }
    };

    static final ComparableConverter<Float> floatConv = new ComparableConverter<Float>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Float> toComparable(String s) { return new Float(s); }

        public String getStringValue (int docId, FieldData fieldData) {
            return Float.toString(((FloatFieldData)fieldData).floatValue(docId));
        }
    };

    static final ComparableConverter<Double> doubleConv = new ComparableConverter<Double>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<Double> toComparable(String s) { return new Double(s); }

        public String getStringValue (int docId, FieldData fieldData) {
            return Double.toString(((DoubleFieldData)fieldData).doubleValue(docId));
        }
    };

    static final ComparableConverter<String> dateConv = new ComparableConverter<String>() {
        private static final long serialVersionUID = ComparableConverter.serialVersionUID;

        public Comparable<String> toComparable(String s) {
            return s;
        }

        public String getStringValue (int docId, FieldData fieldData) {
            return ((LongFieldData)fieldData).date(docId).toString();
        }
    };

    static final Map<Class, ComparableConverter> converters =
        Collections.unmodifiableMap(new HashMap<Class, ComparableConverter>() {{
            put(StringFieldMapper.class, stringConv);
            put(ByteFieldMapper.class, byteConv);
            put(ShortFieldMapper.class, shortConv);
            put(IntegerFieldMapper.class, intConv);
            put(LongFieldMapper.class, longConv);
            put(FloatFieldMapper.class, floatConv);
            put(DoubleFieldMapper.class, doubleConv);

            // These are types that only exist in ES upper layer.
            // For example, FieldDataType does not differentiate Long & Date or Boolean & String
            put(DateFieldMapper.class, dateConv);
            // Boolean share the same entry with String since internal value F < T.
            put(BooleanFieldMapper.class, stringConv);
    }});
}

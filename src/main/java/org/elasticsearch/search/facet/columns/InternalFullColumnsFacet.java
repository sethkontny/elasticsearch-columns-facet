package org.elasticsearch.search.facet.columns;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;

import java.io.IOException;
import java.util.*;

public class InternalFullColumnsFacet extends InternalColumnsFacet {

    private static final String STREAM_TYPE = "fColumns";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readColumnsFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }


    /**
     * A columns facet entry representing a single entry within the result of a histogram facet.
     */
    public static class FullEntry implements Entry {
        String[] keys;
        long key;
        long count;
        long totalCount;
        double total;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        public FullEntry(String[] keys, long key, long count, double min, double max, long totalCount, double total) {
            this.keys = keys;
            this.key = key;
            this.count = count;
            this.min = min;
            this.max = max;
            this.totalCount = totalCount;
            this.total = total;
        }

        //   @Override
        public String[] keys() {
            return keys;
        }

        // @Override
        public String[] getKeys() {
            return keys();
        }

        @Override
        public long key() {
            return key;
        }

        @Override
        public long getKey() {
            return key();
        }

        @Override
        public long count() {
            return count;
        }

        @Override
        public long getCount() {
            return count();
        }

        @Override
        public double total() {
            return total;
        }

        @Override
        public double getTotal() {
            return total();
        }

        @Override
        public long totalCount() {
            return totalCount;
        }

        @Override
        public long getTotalCount() {
            return this.totalCount;
        }

        @Override
        public double mean() {
            return total / totalCount;
        }

        @Override
        public double getMean() {
            return total / totalCount;
        }

        @Override
        public double min() {
            return this.min;
        }

        @Override
        public double getMin() {
            return this.min;
        }

        @Override
        public double max() {
            return this.max;
        }

        @Override
        public double getMax() {
            return this.max;
        }
    }

    private String name;

    private ComparatorType comparatorType;

    ExtTLongObjectHashMap<FullEntry> tEntries;
    boolean cachedEntries;
    Collection<FullEntry> entries;

    private InternalFullColumnsFacet() {
    }

    public InternalFullColumnsFacet(String name, ComparatorType comparatorType, ExtTLongObjectHashMap<InternalFullColumnsFacet.FullEntry> entries, boolean cachedEntries) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.tEntries = entries;
        this.cachedEntries = cachedEntries;
        this.entries = entries.valueCollection();
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return type();
    }

    @Override
    public List<FullEntry> entries() {
        if (!(entries instanceof List)) {
            entries = new ArrayList<FullEntry>(entries);
        }
        return (List<FullEntry>) entries;
    }

    @Override
    public List<FullEntry> getEntries() {
        return entries();
    }

    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) entries().iterator();
    }

    void releaseCache() {
        if (cachedEntries) {
            CacheRecycler.pushLongObjectMap(tEntries);
            cachedEntries = false;
            tEntries = null;
        }
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            // we need to sort it
            InternalFullColumnsFacet internalFacet = (InternalFullColumnsFacet) facets.get(0);
            List<FullEntry> entries = internalFacet.entries();
            Collections.sort(entries, comparatorType.comparator());
            internalFacet.releaseCache();
            return internalFacet;
        }

        ExtTLongObjectHashMap<FullEntry> map = CacheRecycler.popLongObjectMap();

        for (Facet facet : facets) {
            InternalFullColumnsFacet histoFacet = (InternalFullColumnsFacet) facet;
            for (FullEntry fullEntry : histoFacet.entries) {
                FullEntry current = map.get(fullEntry.key);
                if (current != null) {
                    current.count += fullEntry.count;
                    current.total += fullEntry.total;
                    current.totalCount += fullEntry.totalCount;
                    if (fullEntry.min < current.min) {
                        current.min = fullEntry.min;
                    }
                    if (fullEntry.max > current.max) {
                        current.max = fullEntry.max;
                    }
                } else {
                    map.put(fullEntry.key, fullEntry);
                }
            }
            histoFacet.releaseCache();
        }

        // sort
        Object[] values = map.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());
        List<FullEntry> ordered = new ArrayList<FullEntry>(map.size());
        for (int i = 0; i < map.size(); i++) {
            FullEntry value = (FullEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        CacheRecycler.pushLongObjectMap(map);

        // just initialize it as already ordered facet
        InternalFullColumnsFacet ret = new InternalFullColumnsFacet();
        ret.name = name;
        ret.comparatorType = comparatorType;
        ret.entries = ordered;
        return ret;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString KEY = new XContentBuilderString("key");
        static final XContentBuilderString KEYS = new XContentBuilderString("keys");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, ColumnsFacet.TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : entries) {
            builder.startObject();
            builder.field(Fields.KEYS, ((FullEntry)(entry)).getKeys());
            builder.field(Fields.COUNT, entry.count());
            builder.field(Fields.MIN, entry.min());
            builder.field(Fields.MAX, entry.max());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.TOTAL_COUNT, entry.totalCount());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalFullColumnsFacet readColumnsFacet(StreamInput in) throws IOException {
        InternalFullColumnsFacet facet = new InternalFullColumnsFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();

        byte comparatorTypeId = in.readByte();
        if (comparatorTypeId >= 0) {
            comparatorType = ComparatorType.fromId(comparatorTypeId);
        } else {
            int ordersMax = in.readInt();
            Integer[] orders = new Integer[ordersMax];
            boolean[] des = new boolean[ordersMax];
            for (int i = 0; i < ordersMax; i++) {
                orders[i] = in.readInt();
                des[i] = in.readBoolean();
            }
            comparatorType = new ComparatorType((byte)-1, "keys", new MultiFieldsComparator(orders, des));
        }

        cachedEntries = false;
        int size = in.readVInt();
        entries = new ArrayList<FullEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new FullEntry(in.readStringArray(), in.readLong(), in.readVLong(), in.readDouble(), in.readDouble(), in.readVLong(), in.readDouble()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        if (comparatorType.comparator() instanceof MultiFieldsComparator) {
            Integer[] orders = ((MultiFieldsComparator)comparatorType.comparator()).getOrders();
            boolean[] des = ((MultiFieldsComparator)comparatorType.comparator()).getDes();
            out.writeInt(orders.length);
            int idx = 0;
            for (Integer i : orders) {
                out.writeInt(i);
                out.writeBoolean(des[idx]);
                idx++;
            }
        }
        out.writeVInt(entries.size());
        for (FullEntry entry : entries) {
            out.writeStringArray(entry.keys);
            out.writeLong(entry.key);
            out.writeVLong(entry.count);
            out.writeDouble(entry.min);
            out.writeDouble(entry.max);
            out.writeVLong(entry.totalCount);
            out.writeDouble(entry.total);
        }
        releaseCache();
    }
}
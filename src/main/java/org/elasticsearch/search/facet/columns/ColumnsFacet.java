package org.elasticsearch.search.facet.columns;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facet.Facet;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Columns facet.
 */
public interface ColumnsFacet extends Facet, Iterable<ColumnsFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "columns";

    /**
     * An ordered list of histogram facet entries.
     */
    List<? extends Entry> entries();

    /**
     * An ordered list of histogram facet entries.
     */
    List<? extends Entry> getEntries();

    /* This can only handle group by column but not aggregates like min, max, count.
     */
    static class MultiFieldsComparator implements Comparator<Entry>
    {
        final static int CountId = 11;
        final static int TotalId = 12;
        final static int MinId = 13;
        final static int MaxId = 14;
        final static int MeanId = 15;
        final static int AggregatesId = CountId;

        static Map<String, Integer> nameToId = new HashMap<String, Integer>() {{
            put(":count", CountId);
            put(":total", TotalId);
            put(":min", MinId);
            put(":max", MaxId);
            put(":mean", MeanId);
        }};

        private Integer[] orders;

        private boolean[] des; // true if descending order

        public Integer[] getOrders() {
            return orders;
        }

        public boolean[] getDes() {
            return des;
        }

        public MultiFieldsComparator(Integer[] orders, boolean[] des) {
            this.orders = orders;
            this.des = des;
        }

        public int compare(Entry o1, Entry o2) {
            // push nulls to the end
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                }
                return 1;
            }
            if (o2 == null) {
                return -1;
            }

            int idx = 0;
            for (Integer i : orders)
            {
                int c = 0;
                if (i < AggregatesId) {
                    c = o1.keys()[i].compareTo(o2.keys()[i]);
                } else {
                    switch (i) {
                        case CountId:
                            c = (o1.count() < o2.count() ? -1 : (o1.count() == o2.count() ? 0 : 1));
                            break;
                        case TotalId:
                            c = (o1.total() < o2.total() ? -1 : (o1.total() == o2.total() ? 0 : 1));
                            break;
                        case MinId:
                            c = (o1.min() < o2.min() ? -1 : (o1.min() == o2.min() ? 0 : 1));
                            break;
                        case MaxId:
                            c = (o1.max() < o2.max() ? -1 : (o1.max() == o2.max() ? 0 : 1));
                            break;
                        case MeanId:
                            c = (o1.mean() < o2.mean() ? -1 : (o1.mean() == o2.mean() ? 0 : 1));
                            break;
                    }
                }
                if (c != 0) return (des[idx] ? -c : c);
                idx++;
            }
            return 0;
        }

        static public ComparatorType generateComparator(String[] groups, String[] orders)
        {
            Integer[] indexOrders = new Integer[orders.length];
            boolean[] des = new boolean[orders.length];

            int maxGroup = groups.length;
            int orderIdx = 0;
            for (String orderDes : orders)
            {
                String[] order = orderDes.split(" ");
                Integer orderByAggregate = nameToId.get(order[0].toLowerCase());
                if (orderByAggregate != null) {
                    indexOrders[orderIdx] = orderByAggregate;
                } else {
                    for (int i = 0; i < maxGroup; i++) {
                        if (order[0].equals(groups[i])) {
                            indexOrders[orderIdx] = i;
                            break;
                        }
                    }
                }
                des[orderIdx] = order.length >= 2 && order[1].equalsIgnoreCase("desc");
                orderIdx++;
            }

            return generateComparator(indexOrders, des);
        }

        static public ComparatorType generateComparator(Integer[] orders, boolean[] des)
        {
            Comparator comp = new MultiFieldsComparator(orders, des);
            return new ComparatorType((byte)-1, "keys", comp);
        }
    }

    public static class ComparatorType {
        static final ComparatorType KEY = new ComparatorType((byte) 0, "key", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return o1.keys()[0].compareTo(o2.keys()[0]);
            }
        });

        static final ComparatorType COUNT = new ComparatorType((byte) 11, "count", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.count() < o2.count() ? -1 : (o1.count() == o2.count() ? 0 : 1));
            }
        });

        static final ComparatorType TOTAL = new ComparatorType((byte) 12, "total", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.total() < o2.total() ? -1 : (o1.total() == o2.total() ? 0 : 1));
            }
        });


        static final ComparatorType MIN = new ComparatorType((byte) 13, "min", new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.min() < o2.min() ? -1 : (o1.min() == o2.min() ? 0 : 1));
            }
        });

        static final ComparatorType MAX = new ComparatorType((byte) 4, "max", new Comparator<Entry>() {
            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.max() < o2.max() ? -1 : (o1.max() == o2.max() ? 0 : 1));
            }
        });


        static final ComparatorType MEAN = new ComparatorType((byte) 5, "mean", new Comparator<Entry>() {
            @Override
            public int compare(Entry o1, Entry o2) {
                // push nulls to the end
                if (o1 == null) {
                    if (o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return (o1.mean() < o2.mean() ? -1 : (o1.mean() == o2.mean() ? 0 : 1));
            }
        })
                ;


        private final byte id;

        private final String description;

        private final Comparator<Entry> comparator;

        public ComparatorType(byte id, String description, Comparator<Entry> comparator) {
            this.id = id;
            this.description = description;
            this.comparator = comparator;
        }

        public byte id() {
            return this.id;
        }

        public String description() {
            return this.description;
        }

        public Comparator<Entry> comparator() {
            return comparator;
        }

        public static ComparatorType fromId(byte id) {
            switch (id) {
                case 0:
                    return KEY;
                case 11:
                    return COUNT;
                case 12:
                    return TOTAL;
                case 13:
                    return MIN;
                case 14:
                    return MAX;
                case 15:
                    return MEAN;
                default:
                    throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + id + "]");
            }
        }

        public static ComparatorType fromString(String type) {
            if ("key".equals(type)) {
                return KEY;
            } else if ("count".equals(type)) {
                return COUNT;
            } else if ("total".equals(type)) {
                return TOTAL;
            } else if ("min".equals(type)) {
                return MIN;
            } else if ("max".equals(type)) {
                return MAX;
            } else if ("mean".equals(type)) {
                return MEAN;
            } else if ("keys".equals(type)) {
                return null;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + type + "]");
        }
    }

    public interface Entry {

        /**
         * The key value of the histogram.
         */
        String[] keys();

        /**
         * The key value of the histogram.
         */
        String[] getKeys();


        /**
         * The key value of the histogram.
         */
        long key();

        /**
         * The key value of the histogram.
         */
        long getKey();

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        long count();

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        long getCount();

        /**
         * The total count of values aggregated to compute the total.
         */
        long totalCount();

        /**
         * The total count of values aggregated to compute the total.
         */
        long getTotalCount();

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        double total();

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        double getTotal();

        /**
         * The mean of this facet interval.
         */
        double mean();

        /**
         * The mean of this facet interval.
         */
        double getMean();

        /**
         * The minimum value.
         */
        double min();

        /**
         * The minimum value.
         */
        double getMin();

        /**
         * The maximum value.
         */
        double max();

        /**
         * The maximum value.
         */
        double getMax();
    }
}
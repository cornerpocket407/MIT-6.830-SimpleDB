package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    // private final TreeMap<Integer, ArrayList<Integer>> aggregates;
    private final Object aggr;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        // this.aggregates = new TreeMap<>();
        if (this.gbfield == Aggregator.NO_GROUPING) {
            aggr = (Object) new ArrayList<Integer>();
        } else {
            // grouping
            assert gbfieldtype != null;
            if (gbfieldtype == Type.INT_TYPE) {
                aggr = (Object) new TreeMap<Integer, ArrayList<Integer>>();
            } else {
                // must be STRING_TYPE
                aggr = (Object) new TreeMap<String, ArrayList<Integer>>();
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("unchecked")
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbfield == Aggregator.NO_GROUPING) {
            ((ArrayList<Integer>) aggr).add(((IntField)tup.getField(afield)).getValue());
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
                TreeMap<Integer, ArrayList<Integer>> groupAggr = (TreeMap<Integer, ArrayList<Integer>>) aggr;
                Integer gbKey = ((IntField) tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField) tup.getField(afield)).getValue();

                if (!groupAggr.containsKey(gbKey)) {
                    groupAggr.put(gbKey, new ArrayList<>(1));
                }
                groupAggr.get(gbKey).add(aggrVal);
            } else if (gbfieldtype == Type.STRING_TYPE) {
                TreeMap<String, ArrayList<Integer>> groupAggr = (TreeMap<String, ArrayList<Integer>>) aggr;
                String gbKey = ((StringField) tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField) tup.getField(afield)).getValue();

                if (!groupAggr.containsKey(gbKey)) {
                    groupAggr.put(gbKey, new ArrayList<>(1));
                }
                groupAggr.get(gbKey).add(aggrVal);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new AggrDbIterator();
    }

    private class AggrDbIterator implements DbIterator {
        private ArrayList<Tuple> res;
        private Iterator<Tuple> it;

        public int calcAggrRes(ArrayList<Integer> l) {
            assert !l.isEmpty();
            int res = 0;
            switch (what) {
                case MIN:
                    res = l.get(0);
                    for (int v : l) {
                        if (res > v) {
                            res = v;
                        }
                    }
                    break;
                case MAX:
                    res = l.get(0);
                    for (int v : l) {
                        if (res < v) {
                            res = v;
                        }
                    }
                    break;
                case SUM:
                    res = 0;
                    for (int v : l) {
                        res += v;
                    }
                    break;
                case AVG:
                    res = 0;
                    for (int v : l) {
                        res += v;
                    }
                    res = res / l.size();
                    break;
                case COUNT:
                    res = l.size();
                    break;
                case SUM_COUNT:
                    try {
                        throw new DbException("SUM_COUNT: not implemented");
                    } catch (DbException e) {
                        e.printStackTrace();
                    }
                case SC_AVG:
                    try {
                        throw new DbException("SC_AVG: not implemented");
                    } catch (DbException e) {
                        e.printStackTrace();
                    }
            }
            return res;
        }

        @SuppressWarnings("unchecked")
        public AggrDbIterator() {
            res = new ArrayList<Tuple>();
            if (gbfield == Aggregator.NO_GROUPING) {
                Tuple t = new Tuple(getTupleDesc());
                Field aggregateVal = new IntField(this.calcAggrRes((ArrayList<Integer>) aggr));
                t.setField(0, aggregateVal);
                res.add(t);
            } else {
                for (Map.Entry e : ((TreeMap<Integer, ArrayList<Integer>>) aggr).entrySet()) {
                    Tuple t = new Tuple(getTupleDesc());
                    Field groupVal = null;
                    if (gbfieldtype == Type.INT_TYPE) {
                        groupVal = new IntField((int) e.getKey());
                    } else {
                        String str = (String) e.getKey();
                        groupVal = new StringField(str, str.length());
                    }
                    Field aggregateVal = new IntField(this.calcAggrRes((ArrayList<Integer>) e.getValue()));
                    t.setField(0, groupVal);
                    t.setField(1, aggregateVal);
                    res.add(t);
                }
            }
        }

        /**
         * Opens the iterator. This must be called before any of the other methods.
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = res.iterator();
        }

        /**
         * Returns true if the iterator has more tuples.
         *
         * @return true f the iterator has more tuples.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");

            }
            return it.hasNext();

        }

        /**
         * Returns the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return the next tuple in the iteration.
         * @throws NoSuchElementException if there are no more tuples.
         * @throws IllegalStateException  If the iterator has not been opened
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            return it.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException           when rewind is unsupported.
         * @throws IllegalStateException If the iterator has not been opened
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            it = res.iterator();
        }

        /**
         * Returns the TupleDesc associated with this DbIterator.
         *
         * @return the TupleDesc associated with this DbIterator.
         */
        @Override
        public TupleDesc getTupleDesc() {
            if (gbfield == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
        }

        /**
         * Closes the iterator. When the iterator is closed, calling next(),
         * hasNext(), or rewind() should fail by throwing IllegalStateException.
         */
        @Override
        public void close() {
            it = null;
        }
    }

}

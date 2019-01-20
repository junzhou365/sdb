package simpledb;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    public class Stat {
        Integer max;
        Integer min;
        int sum;
        int count;
        int avg;
        public void update(int val) {
            if (max == null || max < val) {
                max = val;
            }
            if (min == null || min > val) {
                min = val;
            }
            sum += val;
            count++;
            avg = sum / count;
        }

        public int get(Op what) {
            switch (what) {
            case MAX:
                return max;

            case MIN:
                return min;

            case SUM:
                return sum;

            case COUNT:
                return count;

            case AVG:
                return avg;
            }

            return 0;
        }

    }

    private Type gbfieldtype;
    private int gbfield;
    private int afield;
    private Op what;
    private Field nongroup;
    private Map<Field, Stat> runnings;

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
        this.gbfieldtype = gbfieldtype;
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;

        runnings = new ConcurrentHashMap<Field, Stat>();
        nongroup = new IntField(0);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field gbf;
        if (gbfieldtype == null) {
            gbf = nongroup;
        } else {
            gbf = tup.getField(gbfield);
        }

        Stat stat = runnings.get(gbf);
        if (stat == null)
            stat = new Stat(){};

        IntField af = (IntField)tup.getField(afield);
        stat.update(af.getValue());
        runnings.put(gbf, stat);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        Tuple t;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfieldtype == null) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            t = new Tuple(td);
            Stat stat = runnings.get(nongroup);
            t.setField(0, new IntField(stat.get(what)));
            tuples.add(t);
            return new TupleIterator(td, tuples);
        }

        td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        for (Map.Entry<Field, Stat> entry: runnings.entrySet()) {
            t = new Tuple(td);
            t.setField(0, entry.getKey());
            t.setField(1, new IntField(entry.getValue().get(what)));
            tuples.add(t);
        }
        TupleIterator ti = new TupleIterator(td, tuples);
        return ti;
    }
}

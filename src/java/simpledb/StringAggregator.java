package simpledb;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;

    private Map<Field, Integer> runnings;
    private Field nongroup;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT)
            throw new IllegalArgumentException();

        runnings = new ConcurrentHashMap<>();
        nongroup = new StringField("", 0);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbf;
        if (gbfieldtype == null) {
            gbf = nongroup;
        } else {
            gbf = tup.getField(gbfield); 
        }

        Integer count = runnings.get(gbf);
        if (count == null)
            count = 0;
        count++;

        runnings.put(gbf, count);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Tuple t;
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfieldtype == null) {
            td = new TupleDesc(new Type[]{Type.STRING_TYPE});
            t = new Tuple(td);
            t.setField(0, new IntField(runnings.get(nongroup)));
            tuples.add(t);
            return new TupleIterator(td, tuples);
        }

        td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        for (Map.Entry<Field, Integer> entry: runnings.entrySet()) {
            t = new Tuple(td);
            t.setField(0, entry.getKey());
            t.setField(1, new IntField(entry.getValue()));
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }
}

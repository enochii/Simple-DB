package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aggField;
    private Op op;

    // gbAttribute -> hashCode?
    private HashMap<Field, List<Tuple>> groups;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.op = what;
        this.groups = new HashMap<>();
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
        //试图抹掉NO_GROUPING和GROUPING的差距
        //TODO: 当没有group可以不用HashMap
        Field key = null;
        if(gbField == NO_GROUPING){
            key = new IntField(NO_GROUPING);
        }else{
            key = tup.getField(gbField);
        }

        //nb
        List<Tuple> group = groups.computeIfAbsent(key, k -> new ArrayList<>());
        group.add(tup);
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
//        Tuple tuple;
        // Integer的Aggregate结果必为int

        AggregateUtil TDUtil = AggregateUtil.getAggUtil(op, gbFieldType, Type.INT_TYPE, this, aggField);
        TupleDesc td = TDUtil.getTupleDesc();

        List<Tuple> res = TDUtil.AggregateEval();

        return new GBIOpIterator(res, td);
    }

    @Override
    public Set<Map.Entry<Field, List<Tuple> > > getEntrySet(){
        return groups.entrySet();
    }

}

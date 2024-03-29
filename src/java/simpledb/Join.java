package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate jp;
    private OpIterator child1;
    private OpIterator child2;

    //Current Tuple in child1, which we need to check
    //cause here we use a nested-loop
    private Tuple next1;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.jp = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(jp.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(jp.getField1());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        next1 = null;
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        next1 = null;

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        next1 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        //上一次的内层循环可能还没跑完.....
        while (next1!=null ||child1.hasNext()){
            if(next1 == null){
                next1 = child1.next();
            }
            while (child2.hasNext()){
                Tuple tuple2 = child2.next();
//                System.out.println("We Are: " + next1+" "+tuple2);
                if(jp.filter(next1, tuple2)){
                    Tuple tuple = new Tuple(getTupleDesc());
                    for(int i=0;i<next1.getNumFields();i++){
                        tuple.setField(i, next1.getField(i));
                    }
                    for(int i=0;i<tuple2.getNumFields();i++){
                        tuple.setField(i+next1.getNumFields(),tuple2.getField(i));
                    }
//                    System.out.println(next1 + " " + tuple2);
                    return tuple;
                }
            }
            //rewind
            next1 = null; //!!!!!!!!!!!!
            child2.rewind();
        }

        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        assert children.length >= 2;
        child1 = children[0];
        child2 = children[1];
    }

}

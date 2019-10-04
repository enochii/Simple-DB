package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
/*
 * 在这里思考的一个小问题是，插入的逻辑应该写在哪里 Insert的构造函数或者是在openLimian
 * 后面有setChild，也就是你应该是可以动态改变你的数据源的，如果逻辑写在构造函数就没有意义了？
 * 写在open的话又会存在副作用，需要写逻辑去避免啥的....
 */

public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TupleDesc td;
    private int insertedNum;
    private Tuple tuple;

    private OpIterator child;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException, TransactionAbortedException {
        // some code goes here
        this.child = child;
        insertedNum = 0;

        child.open();
        while (child.hasNext()){
            try {
                Database.getBufferPool().insertTuple(t,tableId,child.next());
            } catch (IOException e) {
                throw new DbException("IOExpection");
            }
            insertedNum ++;
        }
        child.close();
        td = new TupleDesc(new Type[]{Type.INT_TYPE});
        tuple = new Tuple(td);
        tuple.setField(0,new IntField(insertedNum));
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    int pos = 0;
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        pos = 1;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        pos = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(pos == 0){
            pos++;
            return tuple;
        }else{
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        throw new UnsupportedOperationException();
    }
}

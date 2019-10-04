package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private int pos;
    private TupleDesc td;
    private Tuple tuple;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) throws DbException, TransactionAbortedException {
        // some code goes here
        this.child = child;
        int deleteNum = 0;

        child.open();
        while (child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(t, child.next());
            } catch (IOException e) {
                throw new DbException("IOExpection when deleting Tuples");
            }
            deleteNum ++;
        }

        td = new TupleDesc(new Type[]{Type.INT_TYPE});
        tuple = new Tuple(td);
        tuple.setField(0, new IntField(deleteNum));
        pos = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(pos == 0){
            pos ++;
            return tuple;
        }else {
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

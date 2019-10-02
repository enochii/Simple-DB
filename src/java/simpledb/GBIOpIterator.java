package simpledb;

import java.util.List;
import java.util.NoSuchElementException;

/*
 * GBOpIterator is to translate the Group By Result into OpIterator
 * This Class maybe can use in other places
 */

public class GBIOpIterator implements OpIterator {
    private List<Tuple> tuples;
    int pos;
    TupleDesc td;

    public GBIOpIterator(List<Tuple> gbRes, TupleDesc td){
        tuples = gbRes;
        pos = -1;
        this.td = td;
    }

    public void open()
            throws DbException, TransactionAbortedException{
        pos = 0;
    }

    /** Returns true if the iterator has more tuples.
     * @return true f the iterator has more tuples.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public boolean hasNext() throws DbException, TransactionAbortedException{
        return pos != -1 //isOpen ?
                && pos < tuples.size();
    }

    /**
     * Returns the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return the next tuple in the iteration.
     * @throws NoSuchElementException if there are no more tuples.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
        Tuple ret = null;
        try{
            ret = tuples.get(pos);
        }catch (IndexOutOfBoundsException e){
            throw new NoSuchElementException();
        }

        pos ++;
        return ret;
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException when rewind is unsupported.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public void rewind() throws DbException, TransactionAbortedException{
        pos = 0;
    }

    /**
     * Returns the TupleDesc associated with this OpIterator.
     * @return the TupleDesc associated with this OpIterator.
     */
    public TupleDesc getTupleDesc(){
        return td;
    }

    /**
     * Closes the iterator. When the iterator is closed, calling next(),
     * hasNext(), or rewind() should fail by throwing IllegalStateException.
     */
    public void close(){
        pos = -1;
    }
}

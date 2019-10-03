package simpledb;

import java.io.*;
import java.util.*;

/**
 * A HeapFile object is arranged into a set of pages, each of which consists of a fixed
 * number of bytes for storing tuples, (defined by the constant BufferPool.DEFAULT_PAGE_SIZE)
 * , including a header. In SimpleDB, there is one HeapFile object for each table in the
 * database
 *  每个Page拥有
 */

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    //Here we need to Cache!!!!
    public Page readPage(PageId pid) {
//        BufferPool bufferPool = Database.getBufferPool();
        Page page = null;
//        if((page = bufferPool.isPageCached(pid))!=null)return page;

        try {
            FileInputStream fi = new FileInputStream(file);
            byte[] bytes = new byte[BufferPool.getPageSize()];

            int pgNo = pid.getPageNumber();
            fi.skip(pgNo * BufferPool.getPageSize());
            // read bytes
            long readNum = fi.read(bytes);
            assert readNum == BufferPool.getPageSize();

            page = new HeapPage(new HeapPageId(pid.getTableId(), pgNo), bytes);
        } catch (IOException e) {
            e.printStackTrace();
//            return null;
        }
//        bufferPool.CachePage(pid, page);
        return page;
    }

    // see DbFile.java for javadocs
    public void  writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile ra = new RandomAccessFile(file,"rw");
        byte[] bytes = page.getPageData();

        int pgNo = page.getId().getPageNumber();
        ra.seek(pgNo * BufferPool.getPageSize());
        // write bytes
        ra.write(bytes);
        ra.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
//        List<Page> pages = new ArrayList<>();
        return (int)file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<>();
        int tableId = this.getId();
        int i = 0;
        int numPage = numPages();
        for (;i<numPage;i++){
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(tableId, i),Permissions.READ_WRITE);
            //
            if(page.getNumEmptySlots() == 0)continue;
            page.insertTuple(t);
            page.markDirty(true, tid);
            pages.add(page);
            break;
        }

        if(i == numPage){
            HeapPage newPage = new HeapPage(new HeapPageId(tableId, i), HeapPage.createEmptyPageData());
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            pages.add(newPage);
            writePage(newPage);
        }
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<>();
        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),Permissions.READ_WRITE);

        heapPage.deleteTuple(t);
        heapPage.markDirty(true, tid);

        pages.add(heapPage);
        return pages;
        // not necessary for lab1
    }

    // For Cache Test
//    private List<Tuple> cachedTuples;
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new TupleIterator(this, tid);
    }

    public class TupleIterator implements DbFileIterator{
        HeapFile hf;
        private List<Tuple> tuples;
        private int pos = 0;
        private TransactionId tid;

        TupleIterator(HeapFile hf, TransactionId tid){
            // some code goes here
            this.hf = hf;
            this.tid = tid;
        }
        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open()
                throws DbException, TransactionAbortedException{
//            if(hf.cachedTuples!=null){
//                tuples = hf.cachedTuples;
//                return;
//            }
            int numPg = hf.numPages();
//            System.out.println("NumPage: "+ numPg);
            tuples = new ArrayList<>();
            for(int i=0;i < numPg;i++){
                HeapPageId heapPageId = new HeapPageId(getId(),i);

                HeapPage heapPage = null;
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId,Permissions.READ_ONLY);
//                    heapPage = (HeapPage)readPage(heapPageId);
//                    Database.getBufferPool().CachePage(heapPageId, heapPage);

//                System.out.println(page.getTuples());
                tuples.addAll((heapPage.getTuples()));
            }
//            hf.cachedTuples = tuples;
//            System.out.println("HeapFile Size:" + tuples.size());
        }

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext()
                throws DbException, TransactionAbortedException{
            return tuples != null && pos < tuples.size();
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next()
                throws DbException, TransactionAbortedException{
            Tuple tuple = null;
            try{
//                System.out.println(tuples.size());
                tuple = tuples.get(pos);
            }catch (NullPointerException | NoSuchElementException e){
                throw new NoSuchElementException();
            }

            pos++;
            return tuple;
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException{
            pos = 0;
        }

        /**
         * Closes the iterator.
         */
        public void close(){
            tuples = null;
            pos =0;
        }
    }
}




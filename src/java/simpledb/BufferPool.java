package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.chrono.JapaneseEra.values;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private HashMap<PageId, Page> cache;
    private int pageLimit;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        cache = new HashMap<>();
        pageLimit = numPages;
        LockManager.removeAll();
    }

    private void CachePage(PageId pageId, Page page) throws DbException {
        //if not contains now and pool is full
        //call evictPage()
        if(!cache.containsKey(pageId) && cache.size() >= pageLimit){
            assert cache.size() == pageLimit;
            evictPage();
        }
        cache.put(pageId, page);
    }

//    public Page isPageCached(PageId pageId){
//        return cache.get(pageId);
//    }

    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        //获取Lock 只能在getPage()?
        Page page = cache.get(pid);
        if(page == null){
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            CachePage(pid, page);
        }

//        System.out.println(tid.hashCode() + " " + page.getId().hashCode() + " " + perm);

        Set<Lock> locks = LockManager.getLocks(pid);
        if(locks != null){
            if (perm == Permissions.READ_ONLY){
                boolean flag;
                do{
                    flag = true;
//                    System.out.println(locks.size());
                    for(Lock lock:locks){
                        //如果该页面被其他进程占用
//                        System.out.println("?");
                        if(lock.permissions == Permissions.READ_WRITE && !lock.transactionId.equals(tid)){
//                            System.out.println(lock.transactionId.hashCode() +" | " + tid.hashCode());
                            flag = false;
                        }
                    }
                }while (!flag);
//                System.out.println("R");

            }else {
                boolean flag;
                do{
                    flag = true;
//                    System.out.println(locks.size());
                    for(Lock lock:locks){
                        if(!lock.transactionId.equals(tid)){
                            flag = false;
                        }
                    }
//                    System.out.println("W");
                }while (!flag);
            }
        }

        // some code goes here
        LockManager.addLock(pid, tid,perm);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  synchronized void releasePage(TransactionId tid, PageId pid) throws TransactionAbortedException, DbException {
        // some code goes here
        // not necessary for lab1|lab2
        LockManager.removeLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) throws TransactionAbortedException, DbException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<Lock> locks = LockManager.getLocks(pid);
        for(Lock lock: locks){
            if(lock.transactionId.equals(tid)){
                return true;
            }
        }
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //这个tuple的RecordId应该认为是无效的？
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        updateCachedPages(heapFile.insertTuple(tid, t));
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> updatedPages = heapFile.deleteTuple(tid, t);

        updateCachedPages(updatedPages);
    }

    // 注意要update，因为可能你还没访问过一个Page，然后你插入/删除元组使得他发生了改变
    // 那么你有两个选择，把它cache到buffer pool或者flush到disk
    private void updateCachedPages(List<Page> pages) throws DbException {
        for(Page page:pages){
            CachePage(page.getId(),page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Collection<Page> pages =  cache.values();
        for(Page page: pages){
            flushPage(page);
        }
    }

    private synchronized void flushPage(Page page) throws IOException {
        Database.getCatalog().getDatabaseFile(page.getId()).writePage(page);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    //Hint: remove a page from the buffer pool **without** flushing it to disk.
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        cache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //todo: need to use transaction?
        flushPage(cache.get(pid));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Collection<Page> pagesByTs = cache.values();
        for(Page page:pagesByTs){
            if(tid.equals(page.isDirty())){
                flushPage(page);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException{
        // some code goes here
        // not necessary for lab1
        PageId pageId = evictPolicy();
        Page page = cache.get(pageId);

        try {
            Database.getCatalog().getDatabaseFile(pageId).writePage(page);
        } catch (IOException e) {
            throw new DbException("IOExp when evict");
        }
        discardPage(pageId);
    }

    /*
     * Random Policy................................
     */
    private PageId evictPolicy(){
        Set<PageId> pageIds_ = cache.keySet();
        Object[] pageIds =  pageIds_.toArray();

        int i = Math.abs(new Random().nextInt()) % pageIds.length;
        return (PageId) pageIds[i];
    }
}

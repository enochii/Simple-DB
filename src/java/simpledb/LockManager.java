package simpledb;

import java.util.*;

public class LockManager {
    private  Map<PageId, Set<Lock>> transactionLocks = new HashMap<>();

    public  synchronized void addLock(PageId pageId, TransactionId transactionId, Permissions permissions){
        synchronized (this){
            Set<Lock> locks = transactionLocks.computeIfAbsent(pageId, k -> new HashSet<>());

//        locks.add(new Lock(permissions, transactionId));
            for(Lock lock : locks){
                if ((lock.transactionId.equals(transactionId))){
                    if(permissions == lock.permissions)return;
                    //upgrade
                    if(permissions == Permissions.READ_WRITE){
                        lock.permissions = Permissions.READ_WRITE;
                    }
                    return;
                }
            }
            //Add first time
            locks.add(new Lock(permissions, transactionId));
        }
    }

    public void removeLock(PageId pageId, TransactionId transactionId){
        synchronized (this){
            Set<Lock> locks = getLocks(pageId);
            for(Lock lock: locks){
                if(lock.transactionId.equals(transactionId)){
                    locks.remove(lock);
//                break;
                }
            }
        }
    }

    private synchronized Set<Lock> getLocks(PageId pageId){
        return transactionLocks.get(pageId);
    }

    // ....
    //Lock的管理和BufferPool是默认绑定的
    //至少testcase是这么认为...
    public  synchronized void removeAll(){
        transactionLocks.clear();
    }

    // 没有同步
    public void releaseLocks(TransactionId tid){
        synchronized (this){
            for(Set<Lock> locks : transactionLocks.values()){
//                for(Lock lock : locks){
//                    if(lock.transactionId.equals(tid)){
//                        locks.remove(lock);
//                    }
//                }
                locks.removeIf(lock -> lock.transactionId.equals(tid));
            }
        }
    }

    // Record the pages which some Transaction waits
    private  Map<TransactionId, Set<PageId>> waitList = new HashMap<>();
    /*
     * Detect DeadLock
     */
    public  void waitForPage(TransactionId transactionId, PageId pageId) {
//        transactionIdHashSet.clear();
//        deadLock(transactionId, pageId);

        synchronized (this){
            Set<PageId> pageIds = waitList.computeIfAbsent(transactionId, k -> new HashSet<>());
            pageIds.add(pageId);
        }
    }

    public void deadLockTest(TransactionId transactionId, PageId pageId) throws TransactionAbortedException {
        transactionIdHashSet.clear();
        deadLock(transactionId, pageId);
    }

    public  void cancelWaitState(TransactionId transactionId, PageId pageId){
       synchronized (this){
           Set<PageId> pageIds = waitList.computeIfAbsent(transactionId, k -> new HashSet<>());
           pageIds.remove(pageId);
       }
    }

    public  synchronized Set<PageId> getWaitPages(TransactionId transactionId){
        return waitList.get(transactionId);
    }

    private  HashSet<TransactionId> transactionIdHashSet = new HashSet<>();

    private  void deadLock(TransactionId transactionId, PageId pageId) throws TransactionAbortedException {
        if(transactionIdHashSet.contains(transactionId)){
            throw new TransactionAbortedException();
        }
        transactionIdHashSet.add(transactionId);
        //当前占有该页面的所有事务
        //递归检测
        synchronized (this){
            Set<Lock> locks = getLocks(pageId);
            if(locks == null)return;
            for (Lock lock:locks){
                if(lock.transactionId.equals(transactionId))continue;
                Set<PageId> waitList = getWaitPages(lock.transactionId);
                //如果为空，证明没有在等待的资源(页面)
                if(waitList == null)return;
                for(PageId pageId1:waitList){
                    deadLock(lock.transactionId, pageId1);
                }
            }
        }
    }

    public  void tryToGetPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        Set<Lock> locks = getLocks(pid);
        waitForPage(tid, pid);
//        synchronized (this){
        if(locks != null){
            if (perm == Permissions.READ_ONLY){
                boolean flag;
                do{
                    flag = true;
//                    System.out.println(locks.size());
                    synchronized (this){
                        for(Lock lock:locks){
                            //如果该页面被其他进程占用
//                        System.out.println("?");
                            if(lock.permissions == Permissions.READ_WRITE && !lock.transactionId.equals(tid)){
//                            System.out.println(lock.transactionId.hashCode() +" | " + tid.hashCode());
                                flag = false;
                                break;
                            }
                        }
                    }
                    if(!flag){
//                        waitForPage(tid, pid);
                        deadLockTest(tid, pid);
                    }
                }while (!flag);
//                System.out.println("R");

            }else {
                boolean flag;
                do{
                    flag = true;
//                    System.out.println(locks.size());
                    synchronized (this){
                        for(Lock lock:locks){
                            if(!lock.transactionId.equals(tid)){
                                flag = false;
                                break;
                            }
                        }
                    }

                    if(!flag){
//                        waitForPage(tid, pid);
                        deadLockTest(tid, pid);
                    }
//                    System.out.println("W");
                }while (!flag);
            }
        }
//        }

        // some code goes here
        cancelWaitState(tid, pid);
        addLock(pid, tid,perm);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) throws TransactionAbortedException, DbException {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (this){
            Set<Lock> locks = getLocks(pid);
            for(Lock lock: locks){
                if(lock.transactionId.equals(tid)){
                    return true;
                }
            }
        }
        return false;
    }
}

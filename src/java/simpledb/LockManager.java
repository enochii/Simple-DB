package simpledb;

import java.util.*;

public class LockManager {
    private static Map<PageId, Set<Lock>> transactionLocks = new HashMap<>();

    public synchronized static void addLock(PageId pageId, TransactionId transactionId, Permissions permissions){
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

    public synchronized static void removeLock(PageId pageId, TransactionId transactionId){
        Set<Lock> locks = LockManager.getLocks(pageId);
        for(Lock lock: locks){
            if(lock.transactionId.equals(transactionId)){
                locks.remove(lock);
//                break;
            }
        }
    }

    public static Set<Lock> getLocks(PageId pageId){
        return transactionLocks.get(pageId);
    }

    // ....
    //Lock的管理和BufferPool是默认绑定的
    //至少testcase是这么认为...
    public static void removeAll(){
        transactionLocks.clear();
    }

    // 没有同步
    public  static void releaseLocks(TransactionId tid){
        for(Set<Lock> locks : transactionLocks.values()){
            for(Lock lock : locks){
                if(lock.transactionId.equals(tid)){
                    locks.remove(lock);
                }
            }
        }
    }
}

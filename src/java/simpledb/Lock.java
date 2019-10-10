package simpledb;

public class Lock {
    public Permissions permissions;
    public TransactionId transactionId;

    public Lock(Permissions permissions, TransactionId transactionId){
        this.permissions = permissions;
        this.transactionId = transactionId;
    }
}

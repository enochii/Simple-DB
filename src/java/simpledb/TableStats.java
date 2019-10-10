package simpledb;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int numPages;
    private int numTuples;
//    private HeapFile heapFile;
    private Histogram[] histograms;
    private TupleDesc tupleDesc;
    private DbFileIterator iterator;
    private int ioCostPerPage;

    private Map<Integer, Integer> maxs;
    private Map<Integer, Integer> mins;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        HeapFile heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
        this.numPages = heapFile.numPages();
        this.numTuples = 0;
//        DbFileIterator dbFileIterator =  heapFile.iterator(null);
//        this.heapFile = heapFile;
        this.histograms = new Histogram[heapFile.getTupleDesc().numFields()];
        this.iterator = heapFile.iterator(null);
        this.tupleDesc = heapFile.getTupleDesc();
        this.ioCostPerPage = ioCostPerPage;

        maxs = new HashMap<>();
        mins = new HashMap<>();

        for(int i=0;i<tupleDesc.numFields();i++){
            if(tupleDesc.getFieldType(i) == Type.STRING_TYPE)continue;
            maxs.put(i, Integer.MIN_VALUE);
            mins.put(i, Integer.MAX_VALUE);
        }

        try {
            iterator.open();
            while (iterator.hasNext()){
                numTuples ++;
                Tuple tuple = iterator.next();
                for(int i=0;i<tupleDesc.numFields();i++){
                    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)){
                        IntField intField = (IntField)tuple.getField(i);
                        int val = intField.getValue();
                        if (maxs.get(i) < val){
                            maxs.put(i, val);
                        }
                        if (mins.get(i) > val){
                            mins.put(i, val);
                        }
//                        mins.put(i, Integer.min(mins.get(i), ((IntField)tuple.getField(i)).getValue()));
                    }
                }
            }
            initHistogram();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return histograms[field].avgSelectivity();
    }

    private void initHistogram(){
        int bucket = NUM_HIST_BINS;
//        bucket = bucket > 10?bucket:10;
        try {
//            iterator.open();
            for(int i=0;i<tupleDesc.numFields();i++){
                if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                    histograms[i] = new IntHistogram(bucket,mins.get(i),maxs.get(i));
//                histograms[field] = new IntHistogram(bucket,min,max);
                }else {
                    //todo : more accurate buckets
                    histograms[i] = new StringHistogram(bucket);
                }
            }
            //addValue
            iterator.rewind();
            while (iterator.hasNext()){
                Tuple tuple = iterator.next();
                for(int i=0;i<tupleDesc.numFields();i++){
                    histograms[i].addValue(tuple.getField(i));
                }
            }

        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        return histograms[field].estimateSelectivity(op,constant);
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

}

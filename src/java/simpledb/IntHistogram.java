package simpledb;

import java.util.ArrayList;
import java.util.List;
import static simpledb.Predicate.Op.*;

//import  simpledb.Predicate.Op.GREATER_THAN;
//import static simpledb.Predicate.Op.LESS_THAN_OR_EQ;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // Interval Length per Bucket
    private int intervalLen;
    private List<List<Integer>> fields;
    private int min;
    private int max;
    private int size;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        intervalLen = (max - min) / buckets + 1;
        fields = new ArrayList<>();
        for(int i=0;i<buckets;i++){
            fields.add(new ArrayList<Integer>());
        }
        this.min = min;
        this.max = max;
        this.size = 0;
    }

    /*
     * @param v : Value to add
     * @return : Bucket Index
     */
    private int bucketIndex(int v){
        assert v >= min && v <= max;
        return (v - min) / intervalLen;
    }

    public int size(){
        return size;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        fields.get(bucketIndex(v)).add(v);
        size++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        Estimator estimator = null;
        if(op == LESS_THAN){
            estimator =  new LessThan(this);
        }else if(op == GREATER_THAN){
            estimator = new GreaterThan(this);
        }else if(op == EQUALS){
            estimator = new Equal(this);
        }else if(op == NOT_EQUALS){
            estimator = new NotEqual(this);
        }else if(op == LESS_THAN_OR_EQ){
            estimator = new LessOrEqual(this);
        }else {
            estimator = new GreaterOrEqual(this);
        }
//        assert estimator != null;
        return estimator.estimateSelectivity(op, v);
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder("|");
        int cur = min;
        for(int i=0;i<fields.size();i++){
            sb.append(cur).append("->").append(cur + intervalLen - 1).append("|");
        }

        return sb.toString();
    }

    /*
     * Estimator
     */

    abstract class Estimator {
        IntHistogram intHistogram;
        Estimator(IntHistogram intHistogram){
            this.intHistogram = intHistogram;
        }
        abstract double estimateSelectivity(Predicate.Op op, int v);
    }

    class LessThan extends Estimator{
        LessThan(IntHistogram intHistogram){
            super(intHistogram);
        }
        double estimateSelectivity(Predicate.Op op, int v){
            if(v < min){
                return 0.0;
            }else if(v > max){
                return 1.0;
            }
            int index = bucketIndex(v);
            double equal = fields.get(index).size() / (double)intervalLen;//单位区间长度拥有的记录数量

            double ret = 0;
            for (int i=0;i < index;i++){
                ret += fields.get(i).size();
            }

            ret += equal * (v - index * intervalLen);
            return ret / size();
        }
    }

    class GreaterThan extends Estimator{
        GreaterThan(IntHistogram intHistogram){
            super(intHistogram);
        }
        double estimateSelectivity(Predicate.Op op, int v){
            if(v < min){
                return 1.0;
            }else if(v > max){
                return 0.0;
            }
            int index = bucketIndex(v);
            double equal = fields.get(index).size() / (double)intervalLen;//单位区间长度拥有的记录数量

            double ret = 0;
            for(int i=index+1;i<fields.size();i++){
                ret += fields.get(i).size();
            }
            ret += equal * ((index + 1) * intervalLen-v);// greater than val && in the same bucket as val

            return ret / size();
        }
    }

    class Equal extends Estimator{
        Equal(IntHistogram intHistogram){
            super(intHistogram);
        }
        double estimateSelectivity(Predicate.Op op, int v){
            if(min > v || max < v)return 0.0;
            int index = bucketIndex(v);
            double equal = fields.get(index).size() / (double)intervalLen;//单位区间长度拥有的记录数量
            return equal / size();
        }
    }
    class NotEqual extends Estimator{
        Equal proxy ;
        NotEqual(IntHistogram intHistogram){
            super(intHistogram);
            proxy = new Equal(intHistogram);
        }
        double estimateSelectivity(Predicate.Op op, int v){
            return 1 - proxy.estimateSelectivity(op, v);
        }
    }

    class GreaterOrEqual extends Estimator{
        Equal equal;
        GreaterThan greaterThan;
        GreaterOrEqual(IntHistogram intHistogram){
            super(intHistogram);
            equal = new Equal(intHistogram);
            greaterThan = new GreaterThan(intHistogram);
        }

        double estimateSelectivity(Predicate.Op op, int v){
            return equal.estimateSelectivity(op, v) + greaterThan.estimateSelectivity(op, v);
        }
    }

    class LessOrEqual extends Estimator{
        Equal equal;
        LessThan lessThan;
        LessOrEqual(IntHistogram intHistogram){
            super(intHistogram);
            equal = new Equal(intHistogram);
            lessThan = new LessThan(intHistogram);
        }

        double estimateSelectivity(Predicate.Op op, int v){
            return equal.estimateSelectivity(op, v) + lessThan.estimateSelectivity(op, v);
        }
    }
}


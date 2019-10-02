package simpledb;

import java.util.*;

import static simpledb.Aggregator.Op.*;

/*
 * This Class is  generate the correct Type for Aggregator such like
 * SUM, MAX ......
 */

/*
 * 原来是可以把父类和子类扔一个文件的呀...抽象
 */

/*
 * 其实也可以不用继承，把Max、Min的逻辑都分开写就行了
 */

public abstract class AggregateUtil{
    static public AggregateUtil getAggUtil(Aggregator.Op op, Type group, Type agg,
                                           Aggregator aggregator, int aggField){
        if(op == AVG){
            return new Avg(group, agg, aggregator, aggField);
        }else if(op == SUM){
            return new Sum(group, agg, aggregator, aggField);
        }else if(op == MAX){
            return new Max(group, agg, aggregator, aggField);
        }else if(op == MIN){
            return new Min(group, agg, aggregator, aggField);
        }else if(op == COUNT){
            return new Count(group, agg, aggregator, aggField);
        }

        throw new NoSuchElementException();
    }
//    Aggregator.Op op;
    Aggregator aggregator;
    Type group;
    Type agg;
//    int gbField;
    int aggField;

    Set<Map.Entry<Field, List<Tuple>>> entries;
    AggregateUtil(Type group, Type agg, Aggregator aggregator, int aggField){
        this.group = group;
        this.agg = agg;
//        this.aggregator = aggregator;
        entries = aggregator.getEntrySet();
//        this.gbField = gbField;
        this.aggField = aggField;
    }

    /*
     * Default Version to avoid duplicate codes
     */
    public TupleDesc getTupleDesc(){
        if(group == null){
            return new TupleDesc(new Type[]{agg});
        }
        return new TupleDesc(new Type[]{group, agg});
    }

    abstract public List<Tuple> AggregateEval();

    /*
     * Help Min and Max Compute Aggregate Result
     * Cause the diff between Min and Max is just the compare op!
     */
    List<Tuple> MinMaxHelper(Predicate.Op op){
        assert agg == Type.INT_TYPE;
        List<Tuple> tuples = new ArrayList<>();
        //
        TupleDesc td = this.getTupleDesc();

        for(Map.Entry<Field, List<Tuple>> entry : entries){
            Field key = entry.getKey();
            List<Tuple> vals = entry.getValue();
            //
            Field maxVal = vals.get(0).getField(aggField);
            for(Tuple tuple:vals){
                Field field = tuple.getField(aggField);
                if(maxVal.compare(op, field)){
                    maxVal = field;
                }
            }

            Tuple gbres = new Tuple(td);
            int i = 0;
            if(group != null){
                gbres.setField(i++, key);
            }
            gbres.setField(i, maxVal);

            tuples.add(gbres);
        }
        return tuples;
    }


}


/*
 * Though Max, Min, Avg just support Int...
 * But here i make it look more generic
 */
 class Max extends AggregateUtil{
    public Max(Type group, Type agg, Aggregator aggregator,int aggField){
        super(group, agg,  aggregator,aggField);
    }

    @Override
    public List<Tuple> AggregateEval(){
        return super.MinMaxHelper(Predicate.Op.LESS_THAN);
    }
}

 class Min extends AggregateUtil{
    public Min(Type group, Type agg, Aggregator aggregator ,int aggField){
        super(group, agg, aggregator, aggField);
    }

    @Override
     public List<Tuple> AggregateEval(){
        return super.MinMaxHelper(Predicate.Op.GREATER_THAN);
     }
}

 class Avg extends AggregateUtil {
     public Avg(Type group, Type agg, Aggregator aggregator, int aggField) {
         super(group, agg, aggregator, aggField);
     }

     //可以把sum和avg的逻辑写一起的，但没必要
     @Override
     public List<Tuple> AggregateEval() {
         assert agg == Type.INT_TYPE;
         List<Tuple> tuples = new ArrayList<>();
         //
         TupleDesc td = this.getTupleDesc();

         for (Map.Entry<Field, List<Tuple>> entry : entries) {
             Field key = entry.getKey();
             List<Tuple> vals = entry.getValue();
             //
             int sum = 0;
             for (int i = 0; i < vals.size(); i++) {
                 Tuple tuple = vals.get(i);
                 IntField field = (IntField) tuple.getField(aggField);
                 sum += field.getValue();
             }

             Tuple gbres = new Tuple(td);
             int i = 0;
             if (group != null) {
                 gbres.setField(i++, key);
             }
             assert vals.size() != 0;
             gbres.setField(i, new IntField(sum / vals.size()));

             tuples.add(gbres);
         }
         return tuples;
     }
 }
 class Sum extends AggregateUtil{
    public Sum(Type group, Type agg, Aggregator aggregator ,int aggField){
        super(group, agg,aggregator, aggField);
    }

     @Override
     public List<Tuple> AggregateEval(){
         assert agg == Type.INT_TYPE;
         List<Tuple> tuples = new ArrayList<>();
         //
         TupleDesc td = this.getTupleDesc();

         for(Map.Entry<Field, List<Tuple>> entry : entries){
             Field key = entry.getKey();
             List<Tuple> vals = entry.getValue();
//             System.out.println(key + ": " + vals);
             //
             int sum = 0;
             //哭了，一开始从1开始.......
             for(int i = 0;i< vals.size();i++){
                 Tuple tuple = vals.get(i);
                 IntField field = (IntField)tuple.getField(aggField);
                 sum += field.getValue();
             }

             Tuple gbres = new Tuple(td);
             int i = 0;
             if(group != null){
                 gbres.setField(i++, key);
             }
             gbres.setField(i, new IntField(sum));
//            System.out.print("Sum: " + sum);
             tuples.add(gbres);
         }
//         System.out.println();
         return tuples;
     }
}

 class Count extends AggregateUtil{
    public Count(Type group, Type agg, Aggregator aggregator, int aggField){
        super(group, agg,aggregator, aggField);
    }

     @Override
     public List<Tuple> AggregateEval(){
         List<Tuple> tuples = new ArrayList<>();
         //
         TupleDesc td = this.getTupleDesc();

         for(Map.Entry<Field, List<Tuple>> entry : entries){
             Field key = entry.getKey();
             List<Tuple> vals = entry.getValue();
             //


             Tuple gbres = new Tuple(td);
             int i = 0;
             if(group != null){
                 gbres.setField(i++, key);
             }
             gbres.setField(i, new IntField(vals.size()));

             tuples.add(gbres);
         }
         return tuples;
     }

    @Override
    public TupleDesc getTupleDesc(){
        if(group == null){
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        return new TupleDesc(new Type[]{group, Type.INT_TYPE});
    }
}


package Hive_Join_Optimization_Prototype;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
@SuppressWarnings("Duplicates")
public class Map_side_Join_with_Cache_Count_Reducer extends Reducer <Text, IntWritable, Text, IntWritable>{
    private Map<String, Integer> countMap = new HashMap<String,Integer>();
    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
        int sum=0;
        for (IntWritable value:values){
            sum+=value.get();
        }
        countMap.put(key.toString(), sum);
    }
    public void cleanup(Context context) throws  IOException,InterruptedException{
        Map.Entry<String, Integer> maxByVal = countMap.entrySet()
                .stream()
                .reduce((curr, nxt) -> curr.getValue() > nxt.getValue() ? curr : nxt)
                .get();
        Text key = new Text(maxByVal.getKey());
        IntWritable value = new IntWritable (maxByVal.getValue());
        if(Hive_Join_Optimization_Prototype_Main.getJoinON().contains("A")){
            Hive_Join_Optimization_Prototype_Main.setId(key.toString());
        }
        else if (Hive_Join_Optimization_Prototype_Main.getJoinON().contains("B")){
            Hive_Join_Optimization_Prototype_Main.setCountrycode(key.toString());
        }

        context.write(key,value);
    }
}

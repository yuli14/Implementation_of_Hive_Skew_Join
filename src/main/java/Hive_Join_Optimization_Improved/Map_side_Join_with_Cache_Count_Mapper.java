package Hive_Join_Optimization_Improved;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map_side_Join_with_Cache_Count_Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private int numberOfRecordsToProcess = Hive_Join_Optimization_Improved_Main.getSampleNumber();
    @Override
    public void map(LongWritable key, Text Value, Context context) throws IOException,InterruptedException{
        if (Hive_Join_Optimization_Improved_Main.getJoinON().contains("A")){
            String line= Value.toString();
            String keys = line.split(",")[1];
            context.write(new Text(keys), new IntWritable(1));
        }
        else if (Hive_Join_Optimization_Improved_Main.getJoinON().contains("B")){
            String line= Value.toString();
            String keys = line.split(",")[3];
            context.write(new Text(keys), new IntWritable(1));
        }

    }
    @Override
    public void run(Context context) throws IOException, InterruptedException {

        setup(context);
        int count = 0 ;
        while (context.nextKeyValue()) {
            if(count++<numberOfRecordsToProcess){ // check if enough records has been processed already
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }else{
                break;
            }
        }
    }

//    cleanup(context);
}

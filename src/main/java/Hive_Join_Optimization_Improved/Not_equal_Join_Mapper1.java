package Hive_Join_Optimization_Improved;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class Not_equal_Join_Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //if it is executing A, then send B to reducer
        if (Hive_Join_Optimization_Improved_Main.getJoinON().contains("A")){

            String[] str_value = value.toString().split(",");
            String custID = str_value[0];
            if(!custID.equals(Hive_Join_Optimization_Improved_Main.getId())){
                String Flag = "FlagC";
                context.write(new Text(custID),new Text(Flag));
            }

            }

        //if it's executing B, send A to reducer
        else if(Hive_Join_Optimization_Improved_Main.getJoinON().contains("B")){
            //transfer all information to Reducer, key is the country code
            String[] str_value = value.toString().split(",");

            String countrycode = str_value[4];
            if (!countrycode.equals(Hive_Join_Optimization_Improved_Main.getCountrycode())){
                String Flag = "FlagT";
                context.write(new Text(countrycode),new Text(Flag));
            }


        }
    }


}


package Hive_Join_Optimization_Prototype;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
@SuppressWarnings("Duplicates")
public class Not_equal_Join_Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    public void map (LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
//        System.out.println("mapper1 executed");
        if (Hive_Join_Optimization_Prototype_Main.getJoinON().contains("A")){
            String[] str_value = value.toString().split(",");
            String custID = str_value[1];
            //  Map Transaction table to customer id<>1, flag C, transtotal, countrycode, key is customer id
            if(!custID.equals(Hive_Join_Optimization_Prototype_Main.getId())){

                String transTotal = str_value[2];

                String countrycode = str_value[4];
                String Flag = "FlagT";
                context.write(new Text(custID),new Text(Flag+","+transTotal+","+countrycode));

            }
        }
        else if(Hive_Join_Optimization_Prototype_Main.getJoinON().contains("B")){
//
//            transfer all information to Reducer, key is the country code
//            System.out.println("B executed");
            String[] str_value = value.toString().split(",");

            String countrycode = str_value[4];
            String Flag = "FlagT";
            context.write(new Text(countrycode),new Text(Flag));

        }
    }


}


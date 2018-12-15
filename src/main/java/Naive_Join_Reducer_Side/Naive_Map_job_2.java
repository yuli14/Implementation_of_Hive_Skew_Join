package Naive_Join_Reducer_Side;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//customer id, customer name, age,gender, country code, salary
public class Naive_Map_job_2 extends Mapper<LongWritable,Text,Text,Text>{
    public void map (LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
//        System.out.println("mapper2 executed");
        if (Naive_Join_Reducer_Side_Join_Main.getJoinON().contains("B")){
            String[] str_value = value.toString().split(",");
            String countrycode = str_value[3];
            //  Map customer table to country code, flag C, customer id, salary, key is country code
            String custID = str_value[0];
            String salary = str_value[4];
            String Flag = "FlagC";
//                context.write(new Text(custID),new Text(Flag+","+transTotal+","+numberOfTransItem+","+countrycode));
            context.write(new Text(countrycode),new Text(Flag+","+custID+","+salary));


            }

        else if(Naive_Join_Reducer_Side_Join_Main.getJoinON().contains("A")){
//            transfer customer id and flag to, key is the customer id
            String[] str_value = value.toString().split(",");
            String custID = str_value[0];
            String Flag = "FlagC";
            context.write(new Text(custID),new Text(Flag));

        }
    }

}

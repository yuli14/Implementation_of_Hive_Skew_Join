package Naive_Join_Reducer_Side;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//transaction id, customer id, transaction total, transaction number of items, transaction description
public class Naive_Map_job_1 extends Mapper<LongWritable,Text,Text,Text>{
    public void map (LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        if (Naive_Join_Reducer_Side_Join_Main.getJoinON().contains("A")){
        String[] str_value = value.toString().split(",");
        String custID = str_value[1];
        // Map Transaction table to customer id<>1, flag C, transtotal, countrycode, key is customer id

        String transTotal = str_value[2];

        String countrycode = str_value[4];
        String Flag = "FlagT";
        context.write(new Text(custID),new Text(Flag+","+transTotal+","+countrycode));


    }
        else if(Naive_Join_Reducer_Side_Join_Main.getJoinON().contains("B")){
        //transfer all information to Reducer, key is the country code

        String[] str_value = value.toString().split(",");

        String countrycode = str_value[4];
        String Flag = "FlagT";
        context.write(new Text(countrycode),new Text(Flag));

    }
}
}


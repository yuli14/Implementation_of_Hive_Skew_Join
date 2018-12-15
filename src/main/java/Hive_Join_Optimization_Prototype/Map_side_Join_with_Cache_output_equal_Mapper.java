package Hive_Join_Optimization_Prototype;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("Duplicates")
public class Map_side_Join_with_Cache_output_equal_Mapper extends Mapper<LongWritable, Text, Text, Text>{

//    key is customer id, value is a list of country code
    private HashMap<String,List<String>> customeridMapList = new HashMap<>();
//    key is country code, value is a list of customer id
    private HashMap<String,List<String>> countrycodeMapList = new HashMap<>();
    public void setup(Mapper.Context context) throws IOException, InterruptedIOException {

        String strLineRead = "";

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);

        Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);

        // [0] because we added just one file.
        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(fs.open(dataFile[0])));
        if(Hive_Join_Optimization_Prototype_Main.getJoinON().contains("A")){
            while ((strLineRead = cacheReader.readLine()) != null) {
        // cutomer id, country code
        // Put B.country code =skewed country code into memory
                String customerFieldArray[] = strLineRead.split(",");

               if (customeridMapList.get(customerFieldArray[0])!=null){
                    customeridMapList.get(customerFieldArray[0]).add(customerFieldArray[1]);
                }
                else{
                    List<String> newlist = new ArrayList<String>();
                    newlist.add(customerFieldArray[1]);

                    customeridMapList.put(customerFieldArray[0],newlist);

                }


//                CustomerMap.put(customerid, ci);
            }
        }
        else if(Hive_Join_Optimization_Prototype_Main.getJoinON().contains("B")){
            //coountry code and cutomer id
            while ((strLineRead = cacheReader.readLine()) != null) {

                String transactionFieldArray[] = strLineRead.split(",");
               if (countrycodeMapList.get(transactionFieldArray[0])!=null){
                    countrycodeMapList.get(transactionFieldArray[0]).add(transactionFieldArray[1]);
                }
                else{
                    List<String> newlist = new ArrayList<String>();
                    newlist.add(transactionFieldArray[1]);

                    countrycodeMapList.put(transactionFieldArray[0],newlist);

                }
            }

        }

    }

    public void map (LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //transaction id, customer id, transaction total, transaction number of items, transaction description
        if(Hive_Join_Optimization_Prototype_Main.getJoinON().contains("A")){
            //str_value : id, customer id, total, trans#, country code
            String[] str_value = value.toString().split(",");
            float transTotal = Float.valueOf(str_value[2]);

            String custID = str_value[1];
            //join key is customer id
            if(customeridMapList.get(custID)!=null){
                List<String> countrycodelist = customeridMapList.get(custID);
               for (String countrycode:countrycodelist) {
                    context.write(new Text(custID), new Text(Float.toString(transTotal) + "," + countrycode));
                }

            }

        }
        else if(Hive_Join_Optimization_Prototype_Main.getJoinON().contains("B")){
            //str_value : customer_id, age, gender, country code, salary
            String[] str_value = value.toString().split(",");
            //join key is country code
            String countrycode = str_value[3];

            //in case there is no corresponding countrycode to be joined
            if (countrycodeMapList.get(countrycode)!=null){
                List<String>customeridlist = countrycodeMapList.get(countrycode);
                float salary = Float.valueOf(str_value[4]);
                for (String cid:customeridlist){
                    context.write(new Text(countrycode),new Text(cid+","+Float.toString(salary)));

                }

            }


        }
}

}

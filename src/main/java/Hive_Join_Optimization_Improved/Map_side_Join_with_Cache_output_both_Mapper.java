package Hive_Join_Optimization_Improved;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("Duplicates")

public class Map_side_Join_with_Cache_output_both_Mapper extends Mapper<LongWritable, Text, Text, Text>{


    //    key is customer id, value is a list of country code
    private HashMap<String,List<String>> customeridMapList = new HashMap<>();
    //    key is country code, value is a list of customer id
    private HashMap<String,List<String>> countrycodeMapList = new HashMap<>();

    MultipleOutputs multipleOutputs;
    @Override
    public void setup(Context context) throws IOException, InterruptedIOException {


        multipleOutputs = new MultipleOutputs(context);

        String strLineRead = "";

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);

        Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);

        // [0] because we added just one file.
        BufferedReader cacheReader = new BufferedReader(new InputStreamReader(fs.open(dataFile[0])));
        if(Hive_Join_Optimization_Improved_Main.getJoinON().contains("A")){
            // cutomer id, country code
            // Put B.country code =skewed country code into memory
            while ((strLineRead = cacheReader.readLine()) != null) {

                String customerFieldArray[] = strLineRead.split(",");
                if (customeridMapList.get(customerFieldArray[0])!=null){
                    customeridMapList.get(customerFieldArray[0]).add(customerFieldArray[1]);
                }
                else{
                    List<String> newlist = new ArrayList<String>();
                    newlist.add(customerFieldArray[1]);

                    customeridMapList.put(customerFieldArray[0],newlist);

                }
            }
        }
        else if(Hive_Join_Optimization_Improved_Main.getJoinON().contains("B")){
            //coountry code and cutomer id
            // Put B.country code =skewed country code into memory
            while ((strLineRead = cacheReader.readLine()) != null) {

                String transactionFieldArray[] = strLineRead.split(",");
//                instead of the traditional way where the key countrycode and value customer id are one to one relation,
//                in our scenario, one countrycode can have many customerid, thus instead of save as Hashmap<String,String>, we save it
//                HashMap<String,List<String>>.
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
@Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Table attributes: transaction id, customer id, transaction total, transaction number of items, country code
        if(Hive_Join_Optimization_Improved_Main.getJoinON().contains("A")){
            //str_value : id, customer id, total, trans#, country code
            String[] str_value = value.toString().split(",");

            String custID = str_value[1];
            //join key is customer id
            //transfer the rest of data to reducer
            if(!custID.equals(Hive_Join_Optimization_Improved_Main.getId())){

                String transTotal = str_value[2];
                String countrycode = str_value[4];
                String Flag = "FlagT";
                context.write(new Text(custID),new Text(Flag+","+transTotal+","+countrycode));


            }
            else if(custID.equals(Hive_Join_Optimization_Improved_Main.getId())&&customeridMapList.get(custID)!=null){
                String transTotal = str_value[2];
                if(customeridMapList.get(custID)!=null){
                    List<String> countrycodelist = customeridMapList.get(custID);

                    for (String countrycode:countrycodelist) {
                        multipleOutputs.write("Ais1",new Text(custID),new Text(transTotal+","+countrycode));
                        }

                }


            }

        }
        else if(Hive_Join_Optimization_Improved_Main.getJoinON().contains("B")){
            //str_value : customer_id, age, gender, country code, salary
            String[] str_value = value.toString().split(",");
            //join key is country code
            String countrycode = str_value[3];

            if(!countrycode.equals(Hive_Join_Optimization_Improved_Main.getCountrycode())){

                String custID = str_value[0];
                String salary = str_value[4];
                String Flag = "FlagC";
                context.write(new Text(countrycode),new Text(Flag+","+custID+","+salary));




            }
            //in case there is no corresponding countrycode to be joined
            else if (countrycode.equals(Hive_Join_Optimization_Improved_Main.getCountrycode())&&countrycodeMapList.get(countrycode)!=null){
                float salary = Float.valueOf(str_value[4]);
                if (countrycodeMapList.get(countrycode)!=null){
                    List<String>customeridlist = countrycodeMapList.get(countrycode);

                    for (String cid:customeridlist){
                        multipleOutputs.write("Bis1",new Text(countrycode),new Text(cid+","+Float.toString(salary)));

                    }

                }
            }
        }
    }
@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    multipleOutputs.close();
    }

}

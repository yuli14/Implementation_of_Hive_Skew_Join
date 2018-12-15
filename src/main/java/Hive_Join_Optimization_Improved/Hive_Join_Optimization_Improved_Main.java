package Hive_Join_Optimization_Improved;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.Date;
import java.util.logging.Logger;

@SuppressWarnings("Duplicates")
/**
 * @author Yu Li(yli14@wpi.edu)
 * @author Jiaming Nie(jnie@wpi.edu)
 */

/**
 * This is a improved version of Hive skew join where can handle both situation that the skewed key
 * is known or not. Also, we offer options on join on A.id or B.countrycode. This can be setup in the run function.
 * This method improves the performance of Hive skew join by read table where skewed key is the joined key
 * Once, and read the table to be joined twice.
 *
 * job to execute both
 * select A.id from A join B on A.id = B.id where A.id = 1 and B.id = 1;
 * select A.id from A join B on A.id = B.id where A.id<>1;
 * (we assume id=1 in theory, but in generated dataset, id can be any number between 1 to 50000)

 * or
 * job to execute both
 * select B.countrycode from b join A on B.countrycode = A.countrycode where A.countrycode = 1 and B.countrycode = 1;
 * select B.countrycode from B join A on B.countrycode = A.countrycode where B.countrycode<>1;
 * (we assume countrycode=1 in theory, but in generated dataset, countrycode can be any number between 1 to 1000)

 */

/**
 * @param
 *path: where the data and result is stored, args[0]
 *joinON: either "A" or "B", if "A" then, join on customerid, if "B", then join on countrycode, args[1]
 *id_Known_or_Not: either ("yes" or "Y") or ("no" or "N"), determins whether the skewed key(customerid or countrycode) is known or not, args[2]
 */

/**
 * @return
 * 0 for success, 1 for fail
 */
public class Hive_Join_Optimization_Improved_Main extends Configured implements Tool {
    private static String id = "";
    private static int SampleNumber=0;
    private static String countrycode = "";
    private static String joinON="";
    private static final Logger LOGGER = Logger.getLogger( Hive_Join_Optimization_Improved_Main.class.getName() );

    public static String getJoinON() {
        return joinON;
    }

    public static void setJoinON(String joinON) {
        Hive_Join_Optimization_Improved_Main.joinON = joinON;
    }

    public static String getId() {

        return id;
    }
    public static void setId(String idx) {

        id = idx;
    }


    public static int getSampleNumber(){

        return SampleNumber;
    }
    public static void setSampleNumber(int sm){
        SampleNumber=sm;
    }


    public static String getCountrycode() {

        return countrycode;
    }

    public static void setCountrycode(String countrycode) {

        Hive_Join_Optimization_Improved_Main.countrycode = countrycode;
    }


    public static void main(String[] args) throws Exception
    {
//        if (args.length != 3 ){
//            System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation> >");
//            System.exit(0);
//        }


        int res = ToolRunner.run(new Configuration(), new Hive_Join_Optimization_Improved_Main(), args);
        System.exit(res);
    }

    public Path readfileandsavecachefile(String path, String fname, int[]columns,String skewKey){
        FileWriter fileWriter = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path + fname + ".csv"));
            reader.readLine();
            String line = null;
            fileWriter = new FileWriter(new File(path + fname + "cache.csv"));

            while ((line = reader.readLine()) != null) {

                String item[] = line.split(",");
                String towrite = item[columns[0]];

                if (skewKey.equals(towrite)) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(towrite);
                    for (int i = 1; i <= columns.length - 1; i++) {
                        sb.append(",");
                        sb.append(item[columns[i]]);

                    }
                    sb.append("\n");
                    fileWriter.append(sb.toString());

                }
            }
        }catch (Exception e) {
                e.printStackTrace();
            } finally {

                try {
                    fileWriter.flush();
                    fileWriter.close();
                } catch (IOException e) {
                    System.out.println("Error while flushing/closing fileWriter !!!");
                    e.printStackTrace();
                }

            }


        return new Path(path + fname + "cache.csv");

}



    public int run(String[] args) throws Exception {
        long start = new Date().getTime();
//      Setup the path where data is stored
        String path = "data/";
//      determin which job is executed
        setJoinON("A");
//      Determin whether the id is known or not before we run job
        String id_Known_or_Not = "N";
        if(args.length==3){

            if (args[0]!=null){
                path=args[0];
            }

            if (args[1]!=null){
                setJoinON(args[1]);
            }

            if (args[2]!=null){
                id_Known_or_Not = args[2];
            }
        }


//      Set up the subset of sample size, so that we can know skewed customerid or skewed by
//      only scan part of the Transaction or Customer file.
//      setSampleNumber(Integer.valueOf(args[1]));
        String skewCustomerid = "";
        String skewCountrycode = "";
        long size1 = 0;
        long size2 = 0;
        String csvFile = path+"/skew_id.csv";
        BufferedReader br = null;

        String line = "";
        String cvsSplitBy = ",";
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] skew_id_countrycode = line.split(cvsSplitBy);
                skewCustomerid= skew_id_countrycode[0];
                skewCountrycode = skew_id_countrycode[1];
                size1 = Long.valueOf(skew_id_countrycode[2]);
                size2 = Long.valueOf(skew_id_countrycode[3]);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        setSampleNumber((int)size1/10);
//        setSampleNumber(50000);

//      Setup configurations
        Configuration conf1=new Configuration();
        Configuration c_count = new Configuration();


//      Need to initialize a valid path, otherwise can not be compiled
        Path p1=new Path(path + "/Transaction.csv") ;
        Path p2=new Path(path + "/Customer.csv");
//        Path p3 = new Path(path);
        Path p7 = new Path(path);

        String outputName= "";
//      join by customerid
        if (getJoinON().contains("A")){
            outputName = "Join_on_customer_id";
//          Read transaction table as large table
            p1=new Path(path + "/Transaction.csv") ;

            //put cutomer id and country code from customer table into cache file
            int []columns = {0,3};
            if (id_Known_or_Not.contains("Y") ||id_Known_or_Not.contains("yes"))
            {
                Hive_Join_Optimization_Improved_Main.setId(skewCustomerid);

            }
//          need to run the map reduce job to get to know which id is skewed
            else if (id_Known_or_Not.contains("N") ||id_Known_or_Not.contains("no")){
                Path p3 = new Path(path+"skew_"+"customerid/");
                FileSystem fs = FileSystem.get(c_count);
                if(fs.exists(p3)){
                    fs.delete(p3, true);
                }
//              job to find skew customer id
                Job countjob = new Job(c_count,"get_skew_customerid_or_countrycode");
                countjob.setJarByClass(Hive_Join_Optimization_Improved_Main.class);
                countjob.setMapperClass(Map_side_Join_with_Cache_Count_Mapper.class);
                countjob.setReducerClass(Map_side_Join_with_Cache_Count_Reducer.class);
//              set the skewed customer id in reduce job
                countjob.setOutputKeyClass(Text.class);
                countjob.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(countjob,p1);
                FileOutputFormat.setOutputPath(countjob,p3);
                countjob.waitForCompletion(true);
            }

            String skewkey = Hive_Join_Optimization_Improved_Main.getId();
//          p2 is the two columns of customer.csv, named customercache.csv
//          which can be fit into the distributed cache
            p7 = readfileandsavecachefile(path,"Customer",columns,skewkey);

        }
//      join by country code
        else if (getJoinON().contains("B")){
            outputName = "Join_on_countrycode";
            p1=new Path(path + "/Customer.csv") ;


            //put country code and customer id from transaction table into cache file
            int []columns = {4,1};

            if (id_Known_or_Not.contains("Y") ||id_Known_or_Not.contains("yes"))
            {
                Hive_Join_Optimization_Improved_Main.setCountrycode(skewCountrycode);

            }
//        need to run the map reduce job to get to know which id is skewed
            else if (id_Known_or_Not.contains("N") ||id_Known_or_Not.contains("no")){

                Path p3 = new Path(path+"skewid_"+"countrycode/");

                FileSystem fs = FileSystem.get(c_count);
                if(fs.exists(p3)){
                    fs.delete(p3, true);
                }
//              job to find the skew country code
                Job countjob = new Job(c_count,"get_skew_customerid_or_countrycode");
                countjob.setJarByClass(Hive_Join_Optimization_Improved_Main.class);
//              set the skew country code during executing reduce job
                countjob.setMapperClass(Map_side_Join_with_Cache_Count_Mapper.class);
                countjob.setReducerClass(Map_side_Join_with_Cache_Count_Reducer.class);

                countjob.setOutputKeyClass(Text.class);
                countjob.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(countjob,p1);
                FileOutputFormat.setOutputPath(countjob,p3);
                countjob.waitForCompletion(true);
            }
            String skewkey = Hive_Join_Optimization_Improved_Main.getCountrycode();
            p7 = readfileandsavecachefile(path,"Transaction",columns,skewkey);


        }

        Path p4=new Path(path+"Improved"+outputName+"part/");
        FileSystem fs1 = FileSystem.get(conf1);
        if(fs1.exists(p4)){
            fs1.delete(p4, true);
        }
//      Must add the distributed cache before the job starts
//      p7 is the csv file to be cached, if A, Customercache.csv, if B, Transactioncache.csv
        DistributedCache.addCacheFile(p7.toUri(),conf1);

//      job to execute both select A.id from A join B on A.id = B.id where A.id = 1 and B.id = 1;
//                          select A.id from A join B on A.id = B.id where A.id<>1;

        Job job1 = new Job(conf1,"Job to execute equal selection");
        job1.setJarByClass(Hive_Join_Optimization_Improved_Main.class);

        if (Hive_Join_Optimization_Improved_Main.getJoinON().contains("A")){
            p1=new Path(path + "/Transaction.csv") ;
            Path p5=new Path(path + "/Customer.csv");
//          executing join on A.customerid, then A is the transaction.csv, B is the customer.csv
            MultipleInputs.addInputPath(job1, p1, TextInputFormat.class, Map_side_Join_with_Cache_output_both_Mapper.class);
            MultipleInputs.addInputPath(job1, p5, TextInputFormat.class, Not_equal_Join_Mapper1.class);
//          The name of multiple outputs means that we perform join of A, the Multipleoutput contains all
//          information where A.customerid is the skewed customerid, where the reducer get the rest of information
//          where A.customerid is not the skewed customerid
            MultipleOutputs.addNamedOutput(job1, "Ais1", TextOutputFormat.class, LongWritable.class, Text.class);

        }
        else{
//          executing join on A.countrycode, then A is the customer.csv, B is the transaction.csv
            p1=new Path(path + "/Transaction.csv") ;
            Path p5=new Path(path + "/Customer.csv");
            MultipleInputs.addInputPath(job1, p5, TextInputFormat.class, Map_side_Join_with_Cache_output_both_Mapper.class);
            MultipleInputs.addInputPath(job1, p1, TextInputFormat.class, Not_equal_Join_Mapper1.class);
            MultipleOutputs.addNamedOutput(job1, "Bis1", TextOutputFormat.class, LongWritable.class, Text.class);

        }
        job1.setReducerClass(Not_equal_join_Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, p4);
        job1.waitForCompletion(true);


        boolean success2 = job1.waitForCompletion(true);
//      output the executing time
        long end = new Date().getTime();
        LOGGER.info("Improved job execution on:"+Hive_Join_Optimization_Improved_Main.getJoinON()+" with condition skew key known?:"+id_Known_or_Not);
        LOGGER.info("Job took "+(end-start) + "milliseconds");

//        System.out.println("Job took "+(end-start) + "milliseconds");
        return success2?0:1;
    }

}



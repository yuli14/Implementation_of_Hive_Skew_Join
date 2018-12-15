package Naive_Join_Reducer_Side;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;
import java.util.logging.Logger;

@SuppressWarnings("Duplicates")

/**
 * @author Yu Li(yli14@wpi.edu)
 * @author Jiaming Nie(jnie@wpi.edu)
 */

/**
 * This is the naive solution of skew join, where can be quite slow when the size of
 * data grown larger than 3GB.
 *
 * job to execute
 * select A.id from A join B on A.id = B.id
 * or
 * job to execute
 * select B.countrycode from B join A on A.countrycode = B.countrycode

/**
 * @param
 *path: where the data and result is stored, args[0]
 *joinON: either "A" or "B", if "A" then, join on customerid, if "B", then join on countrycode, args[1]
 */


/**
 * @return
 * 0 for success, 1 for fail
 */
public class Naive_Join_Reducer_Side_Join_Main extends Configured implements Tool{
    private static String joinON="";
    private static final Logger LOGGER = Logger.getLogger( Naive_Join_Reducer_Side_Join_Main.class.getName() );

    public static String getJoinON() {
        return joinON;
    }

    public static void setJoinON(String joinON) {
        Naive_Join_Reducer_Side_Join_Main.joinON = joinON;
    }

    public static void main(String[] args) throws Exception
    {

        int res = ToolRunner.run(new Configuration(), new Naive_Join_Reducer_Side_Join_Main(), args);
        System.exit(res);
//
    }
    public int run(String[] args) throws Exception {
        Long start = new Date().getTime();
        Configuration c=new Configuration();
//      Setup the path where data is stored
        String path = "data/";
        setJoinON("A");
//      Determin whether the id is known or not before we run job
        if(args.length==2){

            if (args[0]!=null){
                path=args[0];
            }

            if (args[1]!=null){
                setJoinON(args[1]);
            }
        }


        Path p1=new Path(path + "/Transaction.csv") ;
        Path p2=new Path(path + "/Customer.csv");
        Path p3=new Path(path+"/output_Naive_Reducer_Side_Join/");


        FileSystem fs = FileSystem.get(c);
        if(fs.exists(p3)){
            fs.delete(p3, true);
        }
        Job job = new Job(c,"Naive Join Reducer Side Join Job");
        job.setJarByClass(Naive_Join_Reducer_Side_Join_Main.class);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, Naive_Map_job_1.class);
        MultipleInputs.addInputPath(job,p2, TextInputFormat.class, Naive_Map_job_2.class);
        job.setReducerClass(Naive_Join_Serialized_Output.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, p3);
        boolean success = job.waitForCompletion(true);
//      output the executing time
        long end = new Date().getTime();
        LOGGER.info("Naive job execution ...");

        LOGGER.info("Job took "+(end-start) + "milliseconds");
//        System.out.println("Job took "+(end-start) + "milliseconds");
        return success?0:1;
    }

}

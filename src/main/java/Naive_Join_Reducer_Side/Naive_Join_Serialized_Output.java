package Naive_Join_Reducer_Side;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
@SuppressWarnings("Duplicates")
public class Naive_Join_Serialized_Output extends Reducer<Text,Text,Text,Text> {

    private ArrayList<Text> listTrans = new ArrayList<Text>();
    private ArrayList<Text> listCust = new ArrayList<Text>();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (Naive_Join_Reducer_Side_Join_Main.getJoinON().contains("A")) {

            listTrans.clear();
            listCust.clear();
            for (Text value : values) {
                String parts[] = value.toString().split(",");
//            System.out.println(t.toString());
                if (parts[0].equals("FlagT")) {

                    listTrans.add(new Text(key.toString() + "," + parts[1] + "," + parts[2]));
                } else if (parts[0].equals("FlagC")) {
                    listCust.add(new Text(key));
                }
            }
//            There is a match
            if (listTrans.size() > 0 && listCust.size() > 0) {
                for (Text t : listTrans) {
                    context.write(null, t);
                }

            }


        } else if (Naive_Join_Reducer_Side_Join_Main.getJoinON().contains("B")) {

            listTrans.clear();
            listCust.clear();
            for (Text value : values) {
                String parts[] = value.toString().split(",");
//            System.out.println(t.toString());
                if (parts[0].equals("FlagT")) {
                    listTrans.add(new Text(key));

                } else if (parts[0].equals("FlagC")) {
                    listCust.add(new Text(key.toString() + "," + parts[1] + "," + parts[2]));

                }
            }
//            There is a match
            if (listTrans.size() > 0 && listCust.size() > 0) {
                for (Text t : listCust) {
                    context.write(null, t);
                }

            }
        }
    }

}

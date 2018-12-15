import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
/**
 * @author Yu Li(yli14@wpi.edu)
 * @author Jiaming Nie(jnie@wpi.edu)
 */

/**
 * This function is used to generate dataset used for the hive join jobs and naive join jobs
 */

/**
 * @param
 *  /path: where the data is going to stored, args[0]
 *  level1: how skew is the Transaction table, eg 7 for 70% args[1]
 *  size1: length of the Transaction table, args[2]
 *  level2: how skew is the  Customer table, args[3]
 *  size2: length of the Customer table, args[4]
 */

public class Generate_Data {
    static SecureRandom rnd = new SecureRandom();
    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";
    private static final int Skew_id = rnd.nextInt(50000);
    private static final int Skew_country_code = rnd.nextInt(999)+1;
    public static void generate_Trans(String path, int level1,long size1){

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(new File(path, "Transaction.csv"));
            for (int i = 0; i < size1; i++) {
                int CustID;
                if (i < size1 * level1 / 10) {
                    CustID = Skew_id;
                } else {
                    CustID = rnd.nextInt(50000);
                }
                int country_code = rnd.nextInt(999)+1;
                int TransNumItems = rnd.nextInt(9) + 1;
                float TransTotal = rnd.nextFloat() * 990 + 10;

                fileWriter.append(String.valueOf(i));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(CustID));
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(String.valueOf(TransTotal));
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(String.valueOf(TransNumItems));
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(String.valueOf(country_code));

                fileWriter.append(NEW_LINE_SEPARATOR);
            }

        } catch (Exception e) {

            System.out.println("Error in CsvFileWriter !!!");

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
    }
    public static void generate_skew_id(String path,long size1,long size2) {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(new File(path, "skew_id.csv"));
            fileWriter.append(Integer.toString(Skew_id));
            fileWriter.append(COMMA_DELIMITER);

            fileWriter.append(Integer.toString(Skew_country_code));
            fileWriter.append(COMMA_DELIMITER);
            fileWriter.append(Long.toString(size1));
            fileWriter.append(COMMA_DELIMITER);
            fileWriter.append(Long.toString(size2));





        } catch (Exception e) {

        System.out.println("Error in CsvFileWriter !!!");

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
    }
    public static void generate_Cust(String path,int level2, long size2){
        String[] gender ={"male","female"};
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(new File(path, "Customer.csv"));

            for (int i=0;i<size2;i++) {
                int countrycode;
                if (i<(level2*size2/10)){
                    countrycode= Skew_country_code;
                }
                else{
                     countrycode = rnd.nextInt(999) + 1;
                }
                Random random = new Random();
                String Gender = gender[random.nextInt(gender.length)];
                int age = rnd.nextInt(60)+10;
                float salary = rnd.nextFloat() * 9900 + 100;

                fileWriter.append(String.valueOf(i));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(String.valueOf(age));
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(Gender);
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(String.valueOf(countrycode));
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(String.valueOf(salary));
                fileWriter.append(NEW_LINE_SEPARATOR);


            }



        } catch (Exception e) {

            System.out.println("Error in CsvFileWriter !!!");

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
    }

    public static void main(String args[]) {
        long size1 = 40000000;

        int level1 = 7;

        long size2 = 40000000;

        int level2 = 7;

        String path = "data/";

        if(args.length==5){
            size1 = new Integer(args[2]);
            level1 = new Integer(args[1]);
            size2 = new Integer(args[4]);
            level2 = new Integer(args[3]);
            path = args[0];

        }

        System.out.println("skew customer id in table transaction is"+Skew_id);
        System.out.println("skew customer country code in table customer is"+Skew_country_code);
        generate_Trans(path,level1,size1);
        generate_Cust(path,level2,size2);
        generate_skew_id(path,size1,size2);

    }
}

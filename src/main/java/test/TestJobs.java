package test;

import configuration.AppConfiguration;
import core.QueryOne;
import core.QueryThree;
import core.QueryTwo;
import utils.HBaseClient;

import java.util.Date;

/**
 * Created by marco on 08/06/17.
 */
public class TestJobs {

    public static int failure = 0;
    public static long first_job = 0;
    public static long second_job = 0;
    public static long third_job = 0;

    public static void main(String[] args) throws Exception {
        AppConfiguration.readConfiguration();
        if (AppConfiguration.HBASE_OUTPUT){
            HBaseClient.createHBaseTable("queryonetable","fi");
            HBaseClient.createHBaseTable("querytwotable","fsi");
            HBaseClient.createHBaseTable("querythreetable","rc");
        }
        System.out.println("\n\n\n test start \n\n\n");
        String[] params = {"test"};
        long start = new Date().getTime();
        QueryOne.main(params);
        long end = new Date().getTime();
        if (failure != 0 ){
            System.out.println("Error in first job");
        }
        else{
            first_job = end-start;
            start = new Date().getTime();
            QueryTwo.main(params);
            end = new Date().getTime();
            if (failure!=0){
                System.out.println("Error in second job");
            }
            else{
                second_job = end-start;
                start = new Date().getTime();
                QueryThree.main(params);
                end = new Date().getTime();
                if (failure!=0){
                    System.out.println("Error in third job");
                }
                else{
                    third_job = end-start;
                    System.out.println("First Job "+ first_job + " milliseconds");
                    System.out.println("Second Job "+second_job + " milliseconds");
                    System.out.println("Third Job "+ third_job + " milliseconds");
                }
            }
        }
        if (AppConfiguration.HBASE_OUTPUT){
            HBaseClient.hbc.scanTable("queryonetable",null,null);
            HBaseClient.hbc.scanTable("querytwotable",null,null);
            HBaseClient.hbc.scanTable("querythreetable",null,null);
        }
        System.exit(failure);



    }
}

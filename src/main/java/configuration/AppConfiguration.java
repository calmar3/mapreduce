package configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Created by marco on 06/06/17.
 */
public class AppConfiguration {

    public static final String FILENAME = "hdfs:///application";


    public static String RATINGS_FILE = "hdfs:///ratings";
    public static String MOVIES_FILE = "hdfs:///movies";
    public static String QUERY_ONE_OUTPUT = "hdfs:///query_one_output";
    public static String QUERY_TWO_PARTIAL = "hdfs:///query_two_partial";
    public static String QUERY_TWO_OUTPUT = "hdfs:///query_two_output";
    public static String QUERY_THREE_PARTIAL_LATEST = "hdfs:///query_three_partial_latest";
    public static String QUERY_THREE_PARTIAL_OLDEST = "hdfs:///query_three_partial_oldest";
    public static String QUERY_THREE_PARTIAL_RANK_OLDEST = "hdfs:///query_three_partial_rank_oldest";
    public static String QUERY_THREE_PARTIAL_RANK_LATEST = "hdfs:///query_three_partial_rank_latest";
    public static String QUERY_THREE_OUTPUT = "hdfs:///query_three_output";
    public static Long QUERY_ONE_TIMESTAMP = Long.valueOf(946684800);
    public static Integer QUERY_ONE_REDUCER = 2;
    public static Integer QUERY_TWO_REDUCER = 1;
    public static Integer QUERY_THREE_REDUCER = 1;
/*    public static Long LATEST_YEAR_START = Long.valueOf(1396310400);
    public static Long LATEST_YEAR_END = Long.valueOf(1396310400);
    public static Long OLDEST_YEAR_START = Long.valueOf(1364774400);
    public static Long OLDEST_YEAR_END = Long.valueOf(1396224000);
    public static Integer RANK_LIMIT = 10;*/

    public static Boolean HBASE_OUTPUT= true;
    public static Boolean HADOOP_OUTPUT= false;

    public static String ZOOKEEPER_HOST = "localhost";
    public static String ZOOKEEPER_PORT = "2181";
    public static String HBASE_MASTER  = "localhost:60000";




    public static void readConfiguration() {

        try {
            Properties prop = new Properties();

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(FILENAME);

            FSDataInputStream in = fs.open(inFile);
            prop.load(in);


            RATINGS_FILE = prop.getProperty("RATINGS_FILE");
            MOVIES_FILE = prop.getProperty("MOVIES_FILE");
            QUERY_ONE_OUTPUT = prop.getProperty("QUERY_ONE_OUTPUT");
            QUERY_TWO_PARTIAL = prop.getProperty("QUERY_TWO_PARTIAL");
            QUERY_TWO_OUTPUT = prop.getProperty("QUERY_TWO_OUTPUT");
            QUERY_THREE_PARTIAL_LATEST = prop.getProperty("QUERY_THREE_PARTIAL_LATEST");
            QUERY_THREE_PARTIAL_OLDEST = prop.getProperty("QUERY_THREE_PARTIAL_OLDEST");
            QUERY_THREE_PARTIAL_RANK_OLDEST = prop.getProperty("QUERY_THREE_PARTIAL_RANK_OLDEST");
            QUERY_THREE_PARTIAL_RANK_LATEST = prop.getProperty("QUERY_THREE_PARTIAL_RANK_LATEST");
            QUERY_THREE_OUTPUT = prop.getProperty("QUERY_THREE_OUTPUT");
            QUERY_ONE_TIMESTAMP = Long.parseLong(prop.getProperty("QUERY_ONE_TIMESTAMP"));
            QUERY_ONE_REDUCER = Integer.parseInt(prop.getProperty("QUERY_ONE_REDUCER"));
            QUERY_TWO_REDUCER = Integer.parseInt(prop.getProperty("QUERY_TWO_REDUCER"));
            QUERY_THREE_REDUCER = Integer.parseInt(prop.getProperty("QUERY_THREE_REDUCER"));
/*            LATEST_YEAR_START = Long.parseLong(prop.getProperty("LATEST_YEAR_START"));
            LATEST_YEAR_END = Long.parseLong(prop.getProperty("LATEST_YEAR_END"));
            OLDEST_YEAR_START = Long.parseLong(prop.getProperty("OLDEST_YEAR_START"));
            OLDEST_YEAR_END = Long.parseLong(prop.getProperty("OLDEST_YEAR_END"));
            RANK_LIMIT =  Integer.parseInt(prop.getProperty("RANK_LIMIT"));*/

            HBASE_OUTPUT= Boolean.parseBoolean(prop.getProperty("HBASE_OUTPUT"));
            HADOOP_OUTPUT= Boolean.parseBoolean(prop.getProperty("HBASE_OUTPUT"));
            ZOOKEEPER_HOST = prop.getProperty("ZOOKEEPER_HOST");
            ZOOKEEPER_PORT = prop.getProperty("ZOOKEEPER_PORT");
            HBASE_MASTER = prop.getProperty("HBASE_MASTER");



        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }



}

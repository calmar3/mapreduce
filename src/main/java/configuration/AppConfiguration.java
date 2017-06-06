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

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }



}

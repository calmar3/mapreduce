package core;

import configuration.AppConfiguration;
import model.QueryThreeRankOutput;
import model.QueryThreeWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import test.TestJobs;
import utils.HBaseClient;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by marco on 01/06/17.
 */
public class QueryThree {

    private static String QUERY_THREE_PARTIAL;
    private static String QUERY_THREE_OUTPUT_RANK;


    public static abstract class GenericPositionMapper extends Mapper<Object, Text, Text, Text> {


        private boolean latest;
        private final static ObjectMapper mapper = new ObjectMapper();

        protected GenericPositionMapper(boolean latest) {
            this.latest = latest;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            String toEmit = new String();
            List<QueryThreeRankOutput> rank = mapper.readValue(line, new TypeReference<List<QueryThreeRankOutput>>() {
            });
            String flag = "o";
            if (latest) {
                flag = "l";
            }
            for (int i = 0; i < rank.size(); i++) {

                toEmit = String.valueOf(i + 1) + ',' + flag;
                context.write(new Text(rank.get(i).getTitle()), new Text(toEmit));
            }
        }
    }

    public static class LatestGenericPositionMapper extends GenericPositionMapper {
        public LatestGenericPositionMapper() {
            super(true);
        }
    }

    public static class OldestGenericPositionMapper extends GenericPositionMapper {
        public OldestGenericPositionMapper() {
            super(false);
        }
    }

    public static class ComparatorReducer extends Reducer<Text, Text, Text, NullWritable> {

        private final static ObjectMapper mapper = new ObjectMapper();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int diff = 0;
            String latest = "";
            String oldest = "";
            for (Text value : values) {
                String line = value.toString().toLowerCase();
                String[] val = line.split(",");
                if (val[1].equals("l")) {
                    latest = val[0];
                } else
                    oldest = val[0];
            }
            if (!latest.equals("")) {
                boolean old = true;
                if (oldest.equals("")) {
                    old = false;
                    oldest = "N/A";
                } else
                    diff = Integer.valueOf(latest) - Integer.valueOf(oldest);
                String toReturn = key.toString() + ",latest: " + latest + ",oldest: " + oldest;
                if (old) {
                    toReturn += ",diff: " + String.valueOf(-diff);
                }

                if (AppConfiguration.HBASE_OUTPUT == true) {
                    context.write(new Text(toReturn), NullWritable.get());
                }

                if (AppConfiguration.HBASE_OUTPUT == true) {

                    HBaseClient.hbc.put("querythreetable", key.toString(), "rc", "latest", latest, "rc", "oldest", oldest, "rc", "diff", Integer.toString(diff));
                }
            }


        }

    }


    public static abstract class GenericThresholdFilterMapper extends Mapper<Object, Text, Text, Text> {

        private long start;
        private long end;
        private final static ObjectMapper mapper = new ObjectMapper();

        protected GenericThresholdFilterMapper(long start, long end) {
            this.end = end;
            this.start = start;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            String[] parts = line.split(",");
            QueryThreeWrapper queryThreeWrapper = new QueryThreeWrapper();
            if (!parts[3].equals("timestamp") &&
                    ((this.start < Long.parseLong(parts[3])) && (Long.parseLong(parts[3]) < this.end))) {
                queryThreeWrapper.setRating(Float.parseFloat(parts[2]));
                if (Float.compare(queryThreeWrapper.getRating(), 0) == 0) {
                    System.out.println(queryThreeWrapper.getRating());
                }
                if (queryThreeWrapper.getRating() != null) {
                    context.write(new Text(parts[1]), new Text(mapper.writeValueAsString(queryThreeWrapper)));
                }
            }
        }
    }

    public static class FirstThresholdFilterMapper extends GenericThresholdFilterMapper {
        public FirstThresholdFilterMapper() {
            super(1396310400, 1427760000);
        }
    }

    public static class SecondThresholdFilterMapper extends GenericThresholdFilterMapper {
        public SecondThresholdFilterMapper() {
            super(1364774400, 1396224000);
        }
    }

    public static class MovieTitleMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            String[] parts = line.split(",");
            if (!(parts[0].equals("movieId"))) {
                QueryThreeWrapper queryThreeWrapper = new QueryThreeWrapper();
                queryThreeWrapper.setTitle(parts[1]);
                context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(queryThreeWrapper)));
            }
        }
    }


    public static class AvgReducer extends Reducer<Text, Text, Text, NullWritable> {

        private final static ObjectMapper mapper = new ObjectMapper();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            float sum = 0;
            int count = 0;
            QueryThreeWrapper returnQueryThreeWrapper = new QueryThreeWrapper();
            for (Text text : values) {
                QueryThreeWrapper queryThreeWrapper = mapper.readValue(text.toString(), QueryThreeWrapper.class);
                if (queryThreeWrapper.getTitle() != null) {
                    returnQueryThreeWrapper.setTitle(queryThreeWrapper.getTitle());
                } else if (queryThreeWrapper.getRating() != null) {

                    sum += queryThreeWrapper.getRating();
                    count++;
                }
            }
            if (count > 0) {
                float avg = (sum / (float) count);
                returnQueryThreeWrapper.setAvg(avg);
                returnQueryThreeWrapper.setRatingsNumber(count);
                context.write(new Text(mapper.writeValueAsString(returnQueryThreeWrapper)), NullWritable.get());
            }


        }
    }

    public static class GeneralRankReducer extends Reducer<Text, Text, Text, NullWritable> {

        private final static ObjectMapper mapper = new ObjectMapper();
        private int limit;

        protected GeneralRankReducer(int limit) {
            this.limit = limit;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            TreeSet<QueryThreeRankOutput> rank = new TreeSet<QueryThreeRankOutput>();
            for (Text value : values) {
                QueryThreeRankOutput queryThreeRankOutput = mapper.readValue(value.toString(), QueryThreeRankOutput.class);
                rank.add(queryThreeRankOutput);
                if (rank.size() > 10 && limit != -1)
                    rank.remove(rank.last());
            }
            context.write(new Text(mapper.writeValueAsString(rank)), NullWritable.get());

        }
    }

    public static class LimitedRankReducer extends GeneralRankReducer {
        public LimitedRankReducer() {
            super(10);
        }
    }

    public static class UnlimitedRankReducer extends GeneralRankReducer {
        public UnlimitedRankReducer() {
            super(-1);
        }
    }

    public static class GeneralMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            QueryThreeWrapper queryThreeWrapper = mapper.readValue(value.toString(), QueryThreeWrapper.class);
            QueryThreeRankOutput queryThreeRankOutput = new QueryThreeRankOutput();
            queryThreeRankOutput.setAvg(queryThreeWrapper.getAvg());
            queryThreeRankOutput.setNumber(queryThreeWrapper.getRatingsNumber());
            queryThreeRankOutput.setTitle(queryThreeWrapper.getTitle());
            context.write(new Text("Rank"), new Text(mapper.writeValueAsString(queryThreeRankOutput)));


        }
    }


    private static int compareRanks() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AvgRatingRank");
        job.setJarByClass(QueryThree.class);


        MultipleInputs.addInputPath(job, new Path(AppConfiguration.QUERY_THREE_PARTIAL_RANK_LATEST), TextInputFormat.class, LatestGenericPositionMapper.class);
        MultipleInputs.addInputPath(job, new Path(AppConfiguration.QUERY_THREE_PARTIAL_RANK_OLDEST), TextInputFormat.class, OldestGenericPositionMapper.class);
        job.setNumReduceTasks(AppConfiguration.QUERY_THREE_REDUCER);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(ComparatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(AppConfiguration.QUERY_THREE_OUTPUT));
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    private static int computeRank(int step) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf, "AvgRatingRank");
        firstJob.setJarByClass(QueryThree.class);
        Class thresholdFilter = FirstThresholdFilterMapper.class;
        Class rankerClass = LimitedRankReducer.class;
        QUERY_THREE_PARTIAL = AppConfiguration.QUERY_THREE_PARTIAL_LATEST;
        QUERY_THREE_OUTPUT_RANK = AppConfiguration.QUERY_THREE_PARTIAL_RANK_LATEST;
        if (step > 0) {
            thresholdFilter = SecondThresholdFilterMapper.class;
            rankerClass = UnlimitedRankReducer.class;
            QUERY_THREE_PARTIAL = AppConfiguration.QUERY_THREE_PARTIAL_OLDEST;
            QUERY_THREE_OUTPUT_RANK = AppConfiguration.QUERY_THREE_PARTIAL_RANK_OLDEST;

        }
        MultipleInputs.addInputPath(firstJob, new Path(AppConfiguration.RATINGS_FILE), TextInputFormat.class, thresholdFilter);
        MultipleInputs.addInputPath(firstJob, new Path(AppConfiguration.MOVIES_FILE), TextInputFormat.class, MovieTitleMapper.class);
        firstJob.setNumReduceTasks(AppConfiguration.QUERY_THREE_REDUCER);
        firstJob.setMapOutputValueClass(Text.class);
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setReducerClass(AvgReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(firstJob, new Path(QUERY_THREE_PARTIAL));
        firstJob.setOutputFormatClass(TextOutputFormat.class);

        int code = firstJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {

            Job secondJob = Job.getInstance(conf, "AvgRatingRank");
            secondJob.setJarByClass(QueryThree.class);
            secondJob.setMapperClass(GeneralMapper.class);
            secondJob.setReducerClass(rankerClass);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setMapOutputKeyClass(Text.class);
            secondJob.setOutputKeyClass(Text.class);
            secondJob.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(secondJob, new Path(QUERY_THREE_PARTIAL));
            FileOutputFormat.setOutputPath(secondJob, new Path(QUERY_THREE_OUTPUT_RANK));
            secondJob.setInputFormatClass(TextInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            code = secondJob.waitForCompletion(true) ? 0 : 2;

        }
        return code;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0)
            AppConfiguration.readConfiguration();
        AppConfiguration.readConfiguration();

        if (AppConfiguration.HBASE_OUTPUT == true && args.length==0) {
            HBaseClient.createHBaseTable("querythreetable","rc");
        }

        int code = computeRank(0);
        if (code == 0) {
            code = computeRank(1);
            if (code == 0) {
                code = compareRanks();
            }
        }
        FileSystem.get(new Configuration()).delete(new Path(AppConfiguration.QUERY_THREE_PARTIAL_LATEST), true);
        FileSystem.get(new Configuration()).delete(new Path(AppConfiguration.QUERY_THREE_PARTIAL_OLDEST), true);
        FileSystem.get(new Configuration()).delete(new Path(AppConfiguration.QUERY_THREE_PARTIAL_RANK_LATEST), true);
        FileSystem.get(new Configuration()).delete(new Path(AppConfiguration.QUERY_THREE_PARTIAL_RANK_OLDEST), true);
        if (args.length > 0) {
            TestJobs.failure = code;
        } else
            System.exit(code);

    }
}

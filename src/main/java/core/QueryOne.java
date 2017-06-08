package core;

import configuration.AppConfiguration;
import model.QueryOneWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import test.TestJobs;

import java.io.IOException;

public class QueryOne {

    public static class TimestampFilterMapper extends Mapper<Object, Text, Text, Text> {


        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String[] parts = line.split(",");
            QueryOneWrapper queryOneWrapper = new QueryOneWrapper();

            if (!parts[3].equals("timestamp") && Long.parseLong(parts[3]) > AppConfiguration.QUERY_ONE_TIMESTAMP){
                queryOneWrapper.setRating(Float.parseFloat(parts[2]));
                if (queryOneWrapper.getRating() != null){
                    context.write(new Text(parts[1]),new Text(mapper.writeValueAsString(queryOneWrapper)) );
                }
            }

        }
    }

    public static class MovieTitleMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String[] parts = line.split(",");
            if (!(parts[0].equals("movieId"))){
                QueryOneWrapper queryOneWrapper = new QueryOneWrapper();
                queryOneWrapper.setTitle(parts[1]);
                context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(queryOneWrapper)) );
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            QueryOneWrapper returnQueryOneWrapper = new QueryOneWrapper();
            for (Text text : values) {
                QueryOneWrapper queryOneWrapper = mapper.readValue(text.toString(), QueryOneWrapper.class);
                if (queryOneWrapper.getTitle() != null){
                    returnQueryOneWrapper.setTitle(queryOneWrapper.getTitle());
                }
                else if (queryOneWrapper.getRating()!=null){
                    sum += queryOneWrapper.getRating();
                    count++;
                }
            }
            float threshold = 4;
            float avg = ( sum / (float) count);
            if (avg >= threshold){
                returnQueryOneWrapper.setRating(avg);
                context.write(key, new Text(mapper.writeValueAsString(returnQueryOneWrapper)));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        /**
         * Read configuration from application.properties file
         */
        if (args.length==0)
            AppConfiguration.readConfiguration();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average rating");
        job.setJarByClass(QueryOne.class);

        MultipleInputs.addInputPath(job, new Path(AppConfiguration.RATINGS_FILE),TextInputFormat.class, TimestampFilterMapper.class);
        MultipleInputs.addInputPath(job, new Path(AppConfiguration.MOVIES_FILE),TextInputFormat.class, MovieTitleMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(AppConfiguration.QUERY_ONE_REDUCER);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        FileOutputFormat.setOutputPath(job, new Path(AppConfiguration.QUERY_ONE_OUTPUT));
        job.setOutputFormatClass(TextOutputFormat.class);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (args.length>0)
            TestJobs.failure = code;
        else
            System.exit(code);

    }
}
package core;

import model.QueryThreeWrapper;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by marco on 01/06/17.
 */
public class QueryThree {


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
                    ((this.start < Long.parseLong(parts[3])) && ( Long.parseLong(parts[3])< this.end))){
                queryThreeWrapper.setRating(Float.parseFloat(parts[2]));
                if (Float.compare(queryThreeWrapper.getRating(),0)==0){
                    System.out.println(queryThreeWrapper.getRating());
                }
                if (queryThreeWrapper.getRating() != null){
                    context.write(new Text(parts[1]),new Text(mapper.writeValueAsString(queryThreeWrapper)) );
                }
            }
        }
    }

    public static class FirstThresholdFilterMapper extends GenericThresholdFilterMapper {
        public FirstThresholdFilterMapper() {
            super(1396310400,1427760000);
        }
    }

    public static class SecondThresholdFilterMapper extends GenericThresholdFilterMapper {
        public SecondThresholdFilterMapper() {
            super(1364774400,1396224000);
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
                QueryThreeWrapper queryThreeWrapper = new QueryThreeWrapper();
                queryThreeWrapper.setTitle(parts[1]);
                context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(queryThreeWrapper)) );
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
                if (queryThreeWrapper.getTitle() != null){
                    returnQueryThreeWrapper.setTitle(queryThreeWrapper.getTitle());
                }
                else if (queryThreeWrapper.getRating()!=null){

                    sum += queryThreeWrapper.getRating();
                    count++;
                }
            }
            if (count > 0  ){
                float avg = ((float) sum / (float) count);
                returnQueryThreeWrapper.setAvg(avg);
                returnQueryThreeWrapper.setRatingsNumber(count);
                context.write(new Text(mapper.writeValueAsString(returnQueryThreeWrapper)),NullWritable.get());
            }


        }
    }

    public static class RankReducer extends Reducer<Text, Text, Text, NullWritable> {

        private final static ObjectMapper mapper = new ObjectMapper();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            List<QueryThreeWrapper> rank = new ArrayList<QueryThreeWrapper>();
            for (Text value : values){
                QueryThreeWrapper queryThreeWrapper = mapper.readValue(value.toString(), QueryThreeWrapper.class);
                if (rank.size() == 0 )
                    rank.add(queryThreeWrapper);
                else {
                    for (QueryThreeWrapper wrapper : rank){
                        if (wrapper.compareTo(queryThreeWrapper)==1)
                            rank.add(rank.indexOf(wrapper),queryThreeWrapper);
                            break;
                    }
                    if (rank.size()<10){
                        rank.add(queryThreeWrapper);
                    }
                    if (rank.size()>10){
                        rank.subList(9,rank.size()-1).clear();
                    }
                }
            }
            for (QueryThreeWrapper wrapper:rank)
                System.out.println(wrapper.toString());
            context.write(new Text(mapper.writeValueAsString(rank)),NullWritable.get());

        }
    }

    public static class GeneralMapper extends Mapper<Object, Text, Text, Text> {


        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            context.write(new Text("Rank"),new Text(value));


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf, "AvgRatingRank");
        firstJob.setJarByClass(QueryThree.class);
        MultipleInputs.addInputPath(firstJob, new Path(args[0]),TextInputFormat.class, FirstThresholdFilterMapper.class);
        MultipleInputs.addInputPath(firstJob, new Path(args[1]),TextInputFormat.class, MovieTitleMapper.class);
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);
        firstJob.setReducerClass(AvgReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(firstJob, new Path(args[2]));
        firstJob.setOutputFormatClass(TextOutputFormat.class);

        int code = firstJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {

            Job secondJob = Job.getInstance(conf, "AvgRatingRank");
            secondJob.setJarByClass(QueryThree.class);
            secondJob.setMapperClass(GeneralMapper.class);
            secondJob.setReducerClass(RankReducer.class);
            secondJob.setMapOutputKeyClass(Text.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(Text.class);
            secondJob.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(secondJob, new Path(args[2] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(secondJob, new Path(args[3]));
            secondJob.setInputFormatClass(TextInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            code = secondJob.waitForCompletion(true) ? 0 : 2;

        }
        System.exit(code);
    }
}

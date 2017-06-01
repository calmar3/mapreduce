package core;

import model.QueryTwoWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import java.util.regex.Pattern;

public class QueryTwo {

    public static class GenresSplitterMapper extends Mapper<Object, Text, Text, FloatWritable> {


        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(Pattern.quote("|"));
            QueryTwoWrapper queryTwoWrapper = mapper.readValue(tokens[1],QueryTwoWrapper.class);
            for (String s : queryTwoWrapper.getGenres()){
                context.write(new Text(s),new FloatWritable(queryTwoWrapper.getRating()));
            }

        }
    }

    /**
     * Calcola la media e la deviazione standard per ogni genere
     */
    public static class ReducerASD extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private final static ObjectMapper mapper = new ObjectMapper();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            int sum = 0;
            for (FloatWritable floatWritable : values){
                sum += floatWritable.get();
                count++;
            }
            float avg = (float) sum / (float) count;

            context.write(key,new FloatWritable(avg));
        }
    }

    /**
     * Emette con chiave movieId oggetti aventi
     * i generi del film se presenti
     */
    public static class FilterNoGenresMoviesMapper extends Mapper<Object, Text, Text, Text> {


        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(",");
            if (!(parts[0].equals("movieId"))){
                QueryTwoWrapper queryTwoWrapper = new QueryTwoWrapper();
                queryTwoWrapper.setGenres(parts[2].split(Pattern.quote("|")));
                if (queryTwoWrapper.getGenres().length != 1 && !(queryTwoWrapper.getGenres()[0].equals("(no genres listed)"))){
                    context.write(new Text(parts[0]),new Text(new Text(mapper.writeValueAsString(queryTwoWrapper))));
                }
            }

        }
    }

    /**
     * Emette con chiave movieId i rating relativi al film
     */
    public static class RatingsMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            String[] parts = line.split(",");
            QueryTwoWrapper queryTwoWrapper = new QueryTwoWrapper();
            if (!parts[2].equals("rating")){
                queryTwoWrapper.setRating(Float.parseFloat(parts[2]));
                context.write(new Text(parts[1]), new Text(mapper.writeValueAsString(queryTwoWrapper)) );
            }
        }
    }

    /**
     * Emette con chiave movieId
     * oggetti aventi rating del film
     * e generi del film
     */
    public static class JoinerReducer extends Reducer<Text, Text, Text, NullWritable> {

        private final static ObjectMapper mapper = new ObjectMapper();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            QueryTwoWrapper returnQueryTwoWrapper = new QueryTwoWrapper();
            for (Text text : values) {
                QueryTwoWrapper queryTwoWrapper = mapper.readValue(text.toString(), QueryTwoWrapper.class);
                if (queryTwoWrapper.getGenres() != null){
                    returnQueryTwoWrapper.setGenres(queryTwoWrapper.getGenres());
                }
                else if (queryTwoWrapper.getRating()!=null){
                    returnQueryTwoWrapper.setRating(queryTwoWrapper.getRating());
                }
            }
            if (returnQueryTwoWrapper.getGenres()!= null && returnQueryTwoWrapper.getRating()!=null){
                String toReturn = key + "|" + mapper.writeValueAsString(returnQueryTwoWrapper);
                context.write(new Text(toReturn),NullWritable.get());
            }

        }
    }


    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf, "RatingASD");
        firstJob.setJarByClass(QueryOne.class);
        MultipleInputs.addInputPath(firstJob, new Path(args[0]),TextInputFormat.class, RatingsMapper.class);
        MultipleInputs.addInputPath(firstJob, new Path(args[1]),TextInputFormat.class, FilterNoGenresMoviesMapper.class);
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);
        firstJob.setReducerClass(JoinerReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(firstJob, new Path(args[2]));
        firstJob.setOutputFormatClass(TextOutputFormat.class);


        int code = firstJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job secondJob = Job.getInstance(conf, "RatingASD");
            secondJob.setJarByClass(QueryOne.class);
            secondJob.setMapperClass(GenresSplitterMapper.class);
            secondJob.setReducerClass(ReducerASD.class);
            secondJob.setNumReduceTasks(2);
            secondJob.setOutputKeyClass(Text.class);
            secondJob.setOutputValueClass(FloatWritable.class);
            FileInputFormat.addInputPath(secondJob, new Path(args[2] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(secondJob, new Path(args[3]));
            secondJob.setInputFormatClass(TextInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            code = secondJob.waitForCompletion(true) ? 0 : 2;
        }

        System.exit(code);

    }
}
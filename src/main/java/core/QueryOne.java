package core;

import model.Wrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class QueryOne {

    public static class TimestampFilterMapper extends Mapper<Object, Text, Text, Wrapper> {


        private final static long thresholdTimestamp = 946684800;


        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String[] parts = line.split(",");
            Wrapper wrapper = new Wrapper();
            if (!parts[3].equals("timestamp") && Long.parseLong(parts[3]) > thresholdTimestamp)
                wrapper.setFloatWritable(new FloatWritable(Float.parseFloat(parts[2])));
                context.write(new Text(parts[1]),wrapper );

        }
    }

    public static class MovieTitleMapper extends Mapper<Object, Text, Text, Wrapper> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String[] parts = line.split(",");
            Wrapper wrapper = new Wrapper();
            wrapper.setText(new Text(parts[1]));
            context.write(new Text(parts[0]), wrapper );
        }
    }

    public static class AverageReducer extends Reducer<Text, Wrapper, Text, Wrapper> {


        public void reduce(Text key, Iterable<Wrapper> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Wrapper newWrapper = new Wrapper();
            for (Wrapper wrapper : values) {
                if (wrapper.getText()!= null)
                    newWrapper.setText(wrapper.getText());
                else{
                    sum += wrapper.getFloatWritable().get();
                    count++;
                }
            }
            float threshold = 4;
            float avg = ((float) sum / (float) count);
            if (avg >= threshold){
                newWrapper.setFloatWritable(new FloatWritable(avg));
                context.write(key, newWrapper);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        /* Create and configure a new MapReduce Job */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average rating");
        job.setJarByClass(QueryOne.class);

        /* Map function */
       // job.setMapperClass(TimestampFilterMapper.class);
        // if equal to the reduce output, can be omitted
/*        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Wrapper.class);*/

        /* A partitioner can be used to balance load among reducers */

        /* Reduce function */
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, TimestampFilterMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MovieTitleMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // if these files are different from text files, we can specify the format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
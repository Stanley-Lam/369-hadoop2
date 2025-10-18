package csc369;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryRequestCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for access.log
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            if (tokens.length > 0) {
                String hostname = tokens[0].trim();
                context.write(new Text(hostname), new Text("LOG\t1"));
            }
        }
    }

    // Mapper for country.csv
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 2) {
                String hostname = fields[0].trim();
                String country = fields[1].trim();
                context.write(new Text(hostname), new Text("COUNTRY\t" + country));
            }
        }
    }

    // Reducer for request counts
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String country = null;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("COUNTRY")) {
                    country = parts[1];
                } else if (parts[0].equals("LOG")) {
                    count += Integer.parseInt(parts[1]);
                }
            }

            if (country != null && count > 0) {
                context.write(new Text(country), new IntWritable(count));
            }
        }
    }

    public static class CountryCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String country = parts[0];
                int count = Integer.parseInt(parts[1]);
                context.write(new IntWritable(count), new Text(country));
            }
        }
    }

    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable x = (IntWritable) a;
            IntWritable y = (IntWritable) b;
            return -1 * x.compareTo(y); // reverse sort
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text country : values) {
                context.write(country, key);
            }
        }
    }
}

package csc369;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryUrlCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // Mapper for log
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            if (tokens.length > 6) {
                String hostname = tokens[0];
                String url = tokens[6];
                context.write(new Text(hostname), new Text("LOG\t" + url));
            }
        }
    }

    // Mapper for hostname_country
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text("COUNTRY\t" + parts[1]));
            }
        }
    }


    // Reducer for joining and emitting (country url, 1)
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String country = null;
            List<String> urls = new ArrayList<>();
            System.out.println("hi");

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                if (parts[0].equals("COUNTRY")) {
                    country = parts[1];
                }
                else if (parts[0].equals("LOG")) {
                    urls.add(parts[1]);
                }
            }
            if (country !=  null) {
                for (String url : urls) {
                    context.write(new Text(country + "\t" + url), new IntWritable(1));
                }
            }
        }
    }

    // Mapper for aggregating counts
    public static class IdentityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 3) {
                String country = parts[0];
                String url = parts[1];
                int count = Integer.parseInt(parts[2]);
                context.write(new Text(country + "\t" + url), new IntWritable(count));
            }
        }
    }

    // Reducer for summing counts
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}

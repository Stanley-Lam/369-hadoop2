package csc369;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class CountryRequestCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for access.log
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            if (tokens.length > 0) {
                String hostname = tokens[0];
                context.write(new Text(hostname), new Text("LOG"));
            }
        }
    }

    // Mapper for country.csv
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 2) {
                String hostname = tokens[0];
                String country = tokens[1];
                context.write(new Text(hostname), new Text("Country\t" + country));
            }
        }
    }

    // Reducer for request counts
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

        private Map<String, Integer> countryCounts = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            String country = null;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                if (parts[0].equals("Country")) {
                    country = parts[1];
                }
                else if (parts[0].equals("Logs")) {
                    count++;
                }
            }
            if (country != null && count > 0) {
                countryCounts.put(country, countryCounts.getOrDefault(country, 0) + count);
            }
        }

        public void sorter(Context context) throws IOException, InterruptedException {
            countryCounts.entrySet()
                    .stream()
                    .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                    .forEach(entry -> {
                            try {
                                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                            } catch (IOException | InterruptedException e) {
                                e.printStackTrace();
                            }
                    });
        }
    }
}

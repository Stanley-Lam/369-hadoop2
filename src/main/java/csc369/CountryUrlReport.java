package csc369;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class CountryUrlReport {

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

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 2) {
                context.write(new Text(tokens[0]), new Text("COUNTRY\t" + tokens[1]));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String country = null;
            List<String> urls = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("COUNTRY")) {
                    country = parts[1];
                }
                else if (parts[0].equals("LOG")) {
                    urls.add(parts[1]);
                }
            }

            if (country != null) {
                for (String url : urls) {
                    context.write(new Text(url), new Text(country));
                }
            }

        }
    }

    public static class URLCountryMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length == 2) {
                context.write(new Text(tokens[0]), new Text(tokens[1]));
            }
        }
    }

    public static class CountryListReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text url, Iterable<Text> countries, Context context) throws IOException, InterruptedException {
            Set<String> countrySet = new TreeSet<>();
            for (Text country : countries) {
                countrySet.add(country.toString());
            }

            String joined  = String.join(", ", countrySet);
            context.write(url, new Text(joined));
        }
    }
}

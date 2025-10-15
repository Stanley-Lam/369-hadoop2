package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryCountKey implements WritableComparable<CountryCountKey> {

    private Text country;
    private IntWritable count;

    public CountryCountKey() {
        this.country = new Text();
        this.count = new IntWritable();
    }

    public CountryCountKey(String country, int count) {
        this.country = new Text(country);
        this.count =  new IntWritable(count);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        country.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        country.readFields(input);
        count.readFields(input);
    }

    @Override
    public int compareTo(CountryCountKey other) {
        int comp = this.country.compareTo(other.country);
        if (comp != 0) return comp;
        return -1 * this.count.compareTo(other.count);
    }

    @Override
    public String toString() {
        return country.toString() + "\t" + count.get();
    }

    public Text getCountry() {
        return country;
    }

    public IntWritable getCount() {
        return count;
    }
}

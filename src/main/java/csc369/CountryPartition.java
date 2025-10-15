package csc369;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class CountryPartition extends Partitioner<CountryCountKey, Text> {

    @Override
    public int getPartition(CountryCountKey key, Text value, int numPartitions) {
        return Math.abs(key.getCountry().hashCode()) % numPartitions;
    }
}

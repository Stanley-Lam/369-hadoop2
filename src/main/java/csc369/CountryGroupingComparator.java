package csc369;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CountryGroupingComparator extends WritableComparator {

    protected CountryGroupingComparator() {
        super(CountryCountKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CountryCountKey key1 = (CountryCountKey) w1;
        CountryCountKey key2 = (CountryCountKey) w2;
        return key1.getCountry().compareTo(key2.getCountry());
    }
}

package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("CountryRequestCount".equalsIgnoreCase(otherArgs[0])) {
		Path accessLogPath = new Path(otherArgs[1]);
		Path countryCsvPath = new Path(otherArgs[2]);
		Path tempOutputPath = new Path("temp_output");
		Path finalOutputPath = new Path(otherArgs[3]);

		Job joinJob = Job.getInstance(conf, "Join and Count Requests Per Country");
		joinJob.setJarByClass(HadoopApp.class);

		MultipleInputs.addInputPath(joinJob, accessLogPath, TextInputFormat.class, CountryRequestCount.LogMapper.class);
		MultipleInputs.addInputPath(joinJob, countryCsvPath, TextInputFormat.class, CountryRequestCount.CountryMapper.class);

		joinJob.setReducerClass(CountryRequestCount.JoinReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(joinJob, tempOutputPath);

		if (!joinJob.waitForCompletion(true)) {
			System.exit(1);
		}

		Job sortJob = Job.getInstance(conf, "Sort Countries by Request Count Descending");
		sortJob.setJarByClass(HadoopApp.class);

		sortJob.setMapperClass(CountryRequestCount.CountryCountMapper.class);
		sortJob.setReducerClass(CountryRequestCount.SortReducer.class);
		sortJob.setSortComparatorClass(CountryRequestCount.DescendingIntComparator.class);

		sortJob.setMapOutputKeyClass(IntWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(sortJob, tempOutputPath);
		FileOutputFormat.setOutputPath(sortJob, finalOutputPath);

		System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
	}
	else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        //System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}

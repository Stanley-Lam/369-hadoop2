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

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryRequestCount".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, CountryRequestCount.LogMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, CountryRequestCount.CountryMapper.class);

		job.setReducerClass(CountryRequestCount.JoinReducer.class);

		job.setOutputKeyClass(CountryRequestCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryRequestCount.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("CountryUrlCount".equalsIgnoreCase(otherArgs[0])) {

		Path logInput = new Path(otherArgs[1]);
		Path countryInput = new Path(otherArgs[2]);
		Path intermediateOutput = new Path("intermediate_country_url");
		Path finalOutput = new Path(otherArgs[3]);

		Job joinJob = new Job(conf, "Join Logs with Country");
		joinJob.setJarByClass(HadoopApp.class);

		MultipleInputs.addInputPath(joinJob, logInput, TextInputFormat.class, CountryUrlCount.LogMapper.class);
		MultipleInputs.addInputPath(joinJob, countryInput, TextInputFormat.class, CountryUrlCount.CountryMapper.class);

		joinJob.setReducerClass(CountryUrlCount.JoinReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(joinJob, intermediateOutput);

		if (!joinJob.waitForCompletion(true)) {
			System.err.println("Join phase failed.");
			System.exit(1);
		}

		Job aggregateJob = new Job(conf, "Aggregate Country URL Counts");
		aggregateJob.setJarByClass(HadoopApp.class);

		aggregateJob.setMapperClass(CountryUrlCount.IdentityMapper.class);
		aggregateJob.setReducerClass(CountryUrlCount.SumReducer.class);
		aggregateJob.setOutputKeyClass(Text.class);
		aggregateJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(aggregateJob, intermediateOutput);
		FileOutputFormat.setOutputPath(aggregateJob, finalOutput);

		System.exit(aggregateJob.waitForCompletion(true) ? 0 : 1);
	} else if ("CountryUrlReport".equalsIgnoreCase(otherArgs[0])) {

		Path logInput = new Path(otherArgs[1]);
		Path countryInput = new Path(otherArgs[2]);
		Path intermediateOutput = new Path("intermediate_url_country");
		Path finalOutput = new Path(otherArgs[3]);

		Job joinJob = new Job(conf, "Join Logs with Country");
		joinJob.setJarByClass(HadoopApp.class);

		MultipleInputs.addInputPath(joinJob, logInput, TextInputFormat.class, CountryUrlReport.LogMapper.class);
		MultipleInputs.addInputPath(joinJob, countryInput, TextInputFormat.class, CountryUrlReport.CountryMapper.class);

		joinJob.setReducerClass(CountryUrlReport.JoinReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(joinJob, intermediateOutput);

		if (!joinJob.waitForCompletion(true)) {
			System.err.println("Join phase failed.");
			System.exit(1);
		}

		Job aggregateJob = new Job(conf, "Aggregate Country Lists per URL");
		aggregateJob.setJarByClass(HadoopApp.class);
		aggregateJob.setMapperClass(CountryUrlReport.URLCountryMapper.class);
		aggregateJob.setReducerClass(CountryUrlReport.CountryListReducer.class);
		aggregateJob.setOutputKeyClass(Text.class);
		aggregateJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(aggregateJob, intermediateOutput);
		FileOutputFormat.setOutputPath(aggregateJob, finalOutput);

		System.exit(aggregateJob.waitForCompletion(true) ? 0 : 1);
	}
	else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}

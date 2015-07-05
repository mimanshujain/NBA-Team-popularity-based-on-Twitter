import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A simple map reduce job that inserts word counts into accumulo. See the README for instructions on how to run this.
 * 
 */
public class WordCount extends Configured implements Tool 
{
	private static Options opts;
	private static Option passwordOpt;
	private static Option usernameOpt;
	//  private static HashMap<String, String> teamMap = TeamNameMap.getTeamMap();
	private static String USAGE = "wordCount <instance name> <zoo keepers> <input dir> <output table>";

	static {
		usernameOpt = new Option("u", "username", true, "username");
		passwordOpt = new Option("p", "password", true, "password");

		opts = new Options();

		opts.addOption(usernameOpt);
		opts.addOption(passwordOpt);
	}


	public int run(String[] unprocessed_args) throws Exception {
		Parser p = new BasicParser();

		CommandLine cl = p.parse(opts, unprocessed_args);
		String[] args = cl.getArgs();

		String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
		String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");

		if (args.length != 4) {
			System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 4.");
			return printUsage();
		}

		Job job = new Job(getConf(), WordCount.class.getName());
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[2]));

		job.setMapperClass(MapClass.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username, password.getBytes(), true, args[3]);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
		job.waitForCompletion(true);

		Instance inst = new ZooKeeperInstance(args[0], args[1]);
		Connector conn = inst.getConnector(args[3], args[3]); //args[3] is user name
		Authorizations auths = new Authorizations();
		org.apache.accumulo.core.client.Scanner scanWin = conn.createScanner("win", auths);	
		org.apache.accumulo.core.client.Scanner scanLose = conn.createScanner("lose", auths);
		
		System.out.println("-------------------------------Win Ranking-------------------------------");
		for(Entry<Key, Value> entry : scanWin) {

			String row = entry.getKey().getRow().toString();
			String value = entry.getValue().toString();
			System.out.println("Team: " + row + "Wins: " + value);
		}
		
		System.out.println("-------------------------------Lose Ranking-------------------------------");
		for(Entry<Key, Value> entry : scanLose) {

			String row = entry.getKey().getRow().toString();
			String value = entry.getValue().toString();
			System.out.println("Team: " + row + "Wins: " + value);
		}

		return 0;
	}

	private int printUsage() {
		HelpFormatter hf = new HelpFormatter();
		hf.printHelp(USAGE, opts);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new WordCount(), args);
		System.exit(res);
	}
}
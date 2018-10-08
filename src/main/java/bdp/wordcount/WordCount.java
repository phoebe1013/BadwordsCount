package bdp.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

/**
 * A very simple word count :)
 *
 */
public class WordCount 
{

	public static class TokenizerMapper extends Mapper<Object, Text,  Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		Map<String, List<String>> dicmap = new HashMap<>();


		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			String dicPath = config.get("swearwords");
			try {
				BufferedReader br = new BufferedReader(new FileReader(dicPath));
				String line = br.readLine();

				while (line != null) {
					String word[] = line.split(";");
					String key = word[0];
					List<String> meanings = new ArrayList<>();
					for (int i = 1; i < word.length; i++)
						meanings.add(word[i]);
					dicmap.put(key, meanings);   // constructing dictionary model using hashmap
					line = br.readLine();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public List<Map<String, Object>> getjsonContent(String json){

		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<Map<String, Object>> content = getjsonContent(value.toString());


		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		}
	}

    public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
    	conf.set("swearwords", args[2]);
    	Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package bdp.wordcount;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.uttesh.exude.ExudeData;
import com.uttesh.exude.exception.InvalidDataException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount
{

	public static class BadwordCountMapper extends Mapper<Object, Text,  Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Map<String, List<String>> dicmap = new HashMap<>();

		@Override
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			String dicPath = config.get("swearwords");
			try {
				Path path = new Path(dicPath);
				FileSystem fs = path.getFileSystem(config);
				InputStream in = fs.open(path);
				BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
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

		public Map<String, Object> getjsonContent(String json){
			Map<String, Object> map = new HashMap<>();

			JsonObject jo = JSON.parseObject(json, JsonObject.class);
			Field[] fields = jo.getClass().getDeclaredFields();
			try {
				for (Field field : fields) {
					StringBuffer nameOfattr = new StringBuffer(field.getName());
					nameOfattr.setCharAt(0, Character.toUpperCase(nameOfattr.charAt(0)));  // turn the first letter of attribute to uppercase
					Method gettings = jo.getClass().getMethod("get" + nameOfattr);
					Object value = gettings.invoke(jo);
					map.put(nameOfattr.toString(), value);
				}
			} catch (Exception e){
				e.printStackTrace();
			}

			return map;
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, Object> content = getjsonContent(value.toString());
			try {
				String words = ExudeData.getInstance().filterStoppings((String) content.get("Body"));
				String subreddit = (String) content.get("Subreddit");
				String author = (String) content.get("Author");
				boolean isbadbysubre = false;
				boolean isbadbyauthor = false;
				for(String s : words.split("\\s+")){
					if (dicmap.containsKey(s)) {
						if (!isbadbysubre) {
							word.set("subr_" + subreddit);
							context.write(word, one);
							isbadbysubre = true;
						}
						if (!isbadbyauthor) {
							word.set("auth_" + author);
							context.write(word, one);
							isbadbyauthor = true;
						}
						word.set("word_" + s);
						context.write(word, one);
					}
				}
			} catch (InvalidDataException e) {
				e.printStackTrace();
			}
		}
	}

	public static class BadwordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private MultipleOutputs<Text, IntWritable> mos;

		@Override
		public void setup(Context context){
			mos = new MultipleOutputs<>(context);
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values)
				sum += i.get();
			result.set(sum);
			String prefix = key.toString().substring(0, 4);
			String keytext = key.toString().substring(5);
			if (prefix.equals("subr"))
				mos.write("subreddit", new Text(keytext), result);
			else if (prefix.equals("auth"))
				mos.write("author", new Text(keytext), result);
			else
				mos.write("count", new Text(keytext), result);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

    public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
    	conf.set("swearwords", args[2]);

    	Job job = Job.getInstance(conf, "Hatespeech level");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(BadwordCountMapper.class);
	    job.setCombinerClass(BadwordCountReducer.class);
	    job.setReducerClass(BadwordCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    MultipleOutputs.addNamedOutput(job, "subreddit", TextOutputFormat.class, Text.class, IntWritable.class);
	    MultipleOutputs.addNamedOutput(job, "author", TextOutputFormat.class, Text.class, IntWritable.class);
	    MultipleOutputs.addNamedOutput(job, "count", TextOutputFormat.class, Text.class, IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

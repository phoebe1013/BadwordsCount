package bdp.wordcount;


import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.alibaba.fastjson.JSON;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


public class RussianTroll {

    // ===== Same as WordCount
//    public static Map<String, Object> getjsonContent(String json){
//        Map<String, Object> map2 = new HashMap<>();
//
//        JsonObject jo = JSON.parseObject(json, JsonObject.class);
//        Field[] fields = jo.getClass().getDeclaredFields();
//        try {
//            for (Field field : fields) {
//                StringBuffer nameOfattr = new StringBuffer(field.getName());
//                nameOfattr.setCharAt(0, Character.toUpperCase(nameOfattr.charAt(0)));  // turn the first letter of attribute to uppercase
//                Method gettings = jo.getClass().getMethod("get" + nameOfattr);
//                Object value = gettings.invoke(jo);
//                map2.put(nameOfattr.toString(), value);
//            }
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//
//        return map2;
//    }


    public static class RussianCountMapper extends Mapper<Object, Text,  Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ArrayList<String> reddit_russian = new ArrayList<>();

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            String dicPath = config.get("russianTroll"); // ========

            try {
                Path path = new Path(dicPath);
                InputStream in = path.getFileSystem(config).open(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));  // UTF_8??
                String line = br.readLine();

                while (line != null) {
                    String name = line.substring(2);
                    reddit_russian.add(name);
                    line = br.readLine();  // ======? close
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Object> content = WordCount.getjsonContent(value.toString());
//            Map<String, Object> content = getjsonContent(value.toString());
            String author = (String) content.get("Author");
            if (reddit_russian.contains(author)) {    // check if this is Russian Troll account
                String subreddit = (String) content.get("Subreddit");
                String createUTC = (String) content.get("Create");

                word.set("subr_" + subreddit);  // count comments/submissions per Russian Troll Account
                context.write(word, one);

                word.set("crea_" + createUTC);  // count create_utc per Russian Troll Account
                context.write(word, one);

            }

        }

    }


    public static class RussianCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private MultipleOutputs<Text, IntWritable> mos;

        @Override
        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable i : values)    // how to sum? didn't separate by sub-name
                sum += i.get();
            result.set(sum);

            String pref = key.toString().substring(0, 4); // why use pref?
            String keytext = key.toString().substring(5); // why keytext? -- Subreddit/Create
            if (pref.equals("subr")) {
                mos.write("subreddit", new Text(keytext), result);
            } else {
                mos.write("author", new Text(keytext), result);
            }

        }


        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();        // close MultipleOutputs<Text, IntWritable> mos
        }


    }


// Comments: "author", "created_utc", "subreddit"
// Submissions: "author", "created_utc", "subreddit"



    public static void main( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
    	conf.set("russiantroll", args[2]);    // Russian Troll

    	Job job = Job.getInstance(conf, "Russian Troll Account");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(RussianCountMapper.class);
	    job.setCombinerClass(RussianCountReducer.class);
	    job.setReducerClass(RussianCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    //
	    MultipleOutputs.addNamedOutput(job, "subreddit", TextOutputFormat.class, Text.class, IntWritable.class);
	    MultipleOutputs.addNamedOutput(job, "author", TextOutputFormat.class, Text.class, IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }






}

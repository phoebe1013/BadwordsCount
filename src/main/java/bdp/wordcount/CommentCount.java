package bdp.wordcount;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class CommentCount {
    public static class NormalizeCountMapper extends Mapper<Object, Text,  Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        HashSet<String> subredditset = new HashSet<>();
        HashSet <String> authorset = new HashSet<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Object> content = WordCount.getjsonContent(value.toString()); // read value --
            String subreddit = (String) content.get("Subreddit");
            String author = (String) content.get("Author");

            if(!subredditset.contains(subreddit)){   //count total # of subreddit
                subredditset.add(subreddit);
                context.write(new Text("sumsubrredit"), one);
            }
            String subredkey = "subr_" + subreddit;  // count # of comments/submissions per subreddit
            context.write(new Text(subredkey), one);

            if (!authorset.contains(author)){  //count total # of author
                authorset.add(author);
                context.write(new Text("sumauthor"), one);
            }

            String authkey = "auth_" + author;  //count # of comments/submissions per author
            context.write(new Text(authkey), one);
        }
    }

    public static class NormalizeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private MultipleOutputs<Text, IntWritable> mos;

        @Override
        public void setup(Context context){
            mos = new MultipleOutputs<>(context);
        }

        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values)
                sum += i.get();
            result.set(sum);    // 1,1,1
            String prefix = key.toString().substring(0, 4);
            String keytext = key.toString().substring(5);

            if (prefix.equals("suma"))
                mos.write("AuthorAmount", new Text("authorAmount"), result);
            else if (prefix.equals("sums"))
                mos.write("SubredditAmount", new Text("subredditAmount"), result);
            else if (prefix.equals("subr"))
                mos.write("subreddit", new Text(keytext), result);
            else
                mos.write("author", new Text(keytext), result);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();        // close MultipleOutputs<Text, IntWritable> mos
        }
    }

//    public static void main( String[] args) throws Exception {
//        Configuration conf = new Configuration();
//
//        Job job = Job.getInstance(conf, "Count subreddit & author");
//        job.setJarByClass(CommentCount.class);
//        job.setMapperClass(CommentCount.NormalizeCountMapper.class);
//        job.setCombinerClass(CommentCount.NormalizeCountReducer.class);
//        job.setReducerClass(CommentCount.NormalizeCountReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        MultipleOutputs.addNamedOutput(job, "subreddit", TextOutputFormat.class, Text.class, IntWritable.class);
//        MultipleOutputs.addNamedOutput(job, "author", TextOutputFormat.class, Text.class, IntWritable.class);
//        MultipleOutputs.addNamedOutput(job, "AuthorAmount", TextOutputFormat.class, Text.class, IntWritable.class);
//        MultipleOutputs.addNamedOutput(job, "SubredditAmount", TextOutputFormat.class, Text.class, IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}

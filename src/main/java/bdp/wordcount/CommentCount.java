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
        private Text word = new Text();
        HashSet<String> subredditset = new HashSet<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Object> content = WordCount.getjsonContent(value.toString());
            String subreddit = (String) content.get("Subrredit");
            word.set(subreddit);
            context.write(word, one);    // How many comments under this subrredit
            if (!subredditset.contains(subreddit)) {
                subredditset.add(subreddit);  // How many subrredit in total
                word.set("subrredit");
                context.write(word, one);
            }
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

        }
    }

    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        conf.set("swearwords", args[2]);

        Job job = Job.getInstance(conf, "Hatespeech level");
        job.setJarByClass(CommentCount.class);
        job.setMapperClass(CommentCount.NormalizeCountMapper.class);
        job.setCombinerClass(CommentCount.NormalizeCountReducer.class);
        job.setReducerClass(CommentCount.NormalizeCountReducer.class);
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

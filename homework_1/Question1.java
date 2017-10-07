/**
 * Created by mac on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Question1 {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("::");
            if (split.length != 3) {
                // this record is in a wrong format, discard it
                return;
            }
            if (split[1].contains("Palo Alto")) {
                String str = split[2].trim();
                for (String cate : str.substring(5, str.length() - 1).split(",")) {
                    context.write(new Text(cate.trim()), NullWritable.get());

                }
            }
        }
    }

    public static class CateReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        // usage: input/business.csv output/
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Question1");
        job.setJarByClass(Question1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(CateReducer.class);
        job.setReducerClass(CateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

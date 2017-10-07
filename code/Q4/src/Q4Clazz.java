import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.HashSet;

public class Q4Clazz extends Configured implements Tool {

    static class AvgMap extends Mapper<LongWritable,Text,Text,DoubleWritable>{

        private HashSet<String> set = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            readFiles(context.getConfiguration());
        }

        protected void readFiles(Configuration conf){
            FileSystem fs = null;
            BufferedReader br = null;
            try {
                Path path = new Path(conf.get("buss"));
                fs = FileSystem.get(conf);
                br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = null;
                while((line = br.readLine())!=null){
                    String[] infos = line.split("::");
                    if(infos[1].contains("Palo Alto")){
                        set.add(infos[0]);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Text userId = new Text();
        private DoubleWritable rate = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("::");
            if(infos.length!=4){
                return;
            }
            if(set.contains(infos[2].trim())){
                userId.set(infos[1]);
                rate.set(Double.parseDouble(infos[3]));
                context.write(userId,rate);
            }
        }
    }

    static class AvgReduce extends Reducer<Text,DoubleWritable,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            BigDecimal count = BigDecimal.ZERO;
            BigDecimal sum = BigDecimal.ZERO;
            for(DoubleWritable value : values){
                count = count.add(BigDecimal.ONE);
                sum = sum.add(BigDecimal.valueOf(value.get()));
            }
            if(!count.equals(BigDecimal.ZERO)){
                double avg = sum.divide(count,BigDecimal.ROUND_HALF_UP).doubleValue();
                context.write(key,new Text(avg+""));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        String cache = args[2];
        Configuration conf = getConf();
        conf.set("buss",cache);
        Job job1 = Job.getInstance(conf,"avg");
        job1.setJarByClass(Q4Clazz.class);
        Path in = new Path(input);
        Path out = new Path(output);
        FileInputFormat.setInputPaths(job1,in);
        FileOutputFormat.setOutputPath(job1,out);
        job1.setMapperClass(AvgMap.class);
        job1.setReducerClass(AvgReduce.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        int res1 = job1.waitForCompletion(true)?0:1;

        return res1;
    }

    public static void main(String [] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new Q4Clazz(),args);
        System.exit(res);
    }
}

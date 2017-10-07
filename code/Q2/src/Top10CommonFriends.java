import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class Top10CommonFriends {

     static class Map extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String infos [] = value.toString().split("\t+");
            if(infos!=null&&infos.length!=0){
                if(infos.length>1){
                    context.write(new Text(infos[0]),new Text(infos[1]));
                }else{
                    context.write(new Text(infos[0]),new Text(""));
                }
            }
        }

     }

    static class Reduce extends Reducer<Text,Text,Text,IntWritable>{

        private Queue<TriplePair> pq = new PriorityQueue<>(new Comparator<TriplePair>() {
            @Override
            public int compare(TriplePair triplePair, TriplePair t1) {
                return triplePair.getCount().get()-t1.getCount().get();
            }
        }
        );
        private static final int k = 10;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TriplePair newPair = new TriplePair();
            String res = "";
            for(Text t :values){
                if(!res.equals("")){
                    res += ",";
                }
                res += t.toString();
            }
            newPair.setCount(new IntWritable(res.split(",").length));
            newPair.setPw(new Text(key));
            newPair.setFriends(new Text(res));
            pq.offer(newPair);
            if(pq.size()>k){
                pq.poll();
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while(!pq.isEmpty()){
                TriplePair pair = pq.poll();
                context.write(new Text(pair.getPw().toString()),new IntWritable(pair.getCount().get()));
            }
        }
    }
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage:<in><out>");
            System.exit(2);
        }
        String input = otherArgs[0];
        Path in = new Path(input);
        String output = otherArgs[1];
        Path out = new Path(output);
        Job job = new Job(conf,"commonfriends");
        job.setJarByClass(Top10FirstPart.class);
        job.setMapperClass(Top10FirstPart.Map.class);
        job.setReducerClass(Top10FirstPart.Reduce.class);
        job.setMapOutputKeyClass(PairWriter.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job, out);
        boolean res = job.waitForCompletion(true);
        if(!res){
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf,"sort");
        job2.setJarByClass(Top10CommonFriends.class);
        job2.setMapperClass(Top10CommonFriends.Map.class);
        job2.setReducerClass(Top10CommonFriends.Reduce.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        Path finalIn = new Path(output+"/part-*");
        FileInputFormat.addInputPath(job2,finalIn);
        FileOutputFormat.setOutputPath(job2, new Path(output+"/top10"));
        System.exit(job2.waitForCompletion(true)?0:1);
    }


}

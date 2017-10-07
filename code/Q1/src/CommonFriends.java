import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class CommonFriends{

    static class Map extends Mapper<LongWritable,Text,PairWriter,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PairWriter pair = new PairWriter();
            Text text = new Text();
            String infos [] = value.toString().split("\t+");
            if(infos!=null&&infos.length>1){
                String host = infos[0];
                String [] friends1 = infos[1].split(",");
                if(friends1.length==1){
                    return;
                }
                for(int i = 0;i<friends1.length;i++){
                    int from = Integer.parseInt(host);
                    int to = Integer.parseInt(friends1[i]);
                    if(from<to){
                        pair.getFrom().set(host.trim());
                        pair.getTo().set(friends1[i].trim());
                    }else if(from > to){
                        pair.getTo().set(host.trim());
                        pair.getFrom().set(friends1[i].trim());
                    }
                    text.set(infos[1].trim());
                    context.write(pair,text);
                }
            }
        }
     }

    static class Reduce extends Reducer<PairWriter,Text,Text,Text>{
        private Text value = new Text();
        private Text newKey = new Text();
        private HashMap<String,String> map = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            map.put("0","4");
            map.put("20","22939");
            map.put("1","29826");
            map.put("6222","19272");
            map.put("28041","28056");
        }

        @Override
        protected void reduce(PairWriter key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fromKey = key.getFrom().toString();
            String toKey = key.getTo().toString();
            if(map.containsKey(fromKey)&&map.get(fromKey).equals(toKey)){
                StringBuffer res = new StringBuffer();
                HashSet<String> set = new HashSet<>();
                for(Text text : values){
                    String [] friends = text.toString().split(",");
                    if(set.size()!=0){
                        for(String friend : friends){
                            if(set.contains(friend)){
                                if(!res.toString().equals("")){
                                    res.append(",");
                                }
                                res.append(friend);
                            }
                        }
                        if(!res.toString().equals("")){
                            value.set(res.toString());
                            newKey.set(key.toString());
                            context.write(newKey,value);
                            map.remove(fromKey);
                        }
                    }else {
                        set.addAll(Arrays.asList(friends));
                    }
                }

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(java.util.Map.Entry entry : map.entrySet()){
                newKey.set(entry.getKey()+","+entry.getValue());
                value.set("");
                context.write(newKey,value);
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

        Job job = new Job(conf,"commonfriends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(PairWriter.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }


}

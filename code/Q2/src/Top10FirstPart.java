import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class Top10FirstPart {

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

        @Override
        protected void reduce(PairWriter key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
                    }
                }else {
                    set.addAll(Arrays.asList(friends));
                }
            }
        }
    }
}

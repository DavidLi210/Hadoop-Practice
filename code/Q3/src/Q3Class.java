import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Q3Class {

    static class AvgMap extends Mapper<LongWritable,Text,Text,DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String infos [] = value.toString().split("::");
            if(infos.length!=4){
                return;
            }
            String bussiness_id = infos[2];
            double rates = Double.parseDouble(infos[3]);
            context.write(new Text(bussiness_id),new DoubleWritable(rates));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    static class AvgReduce extends Reducer<Text,DoubleWritable,Text,Text>{
        private DoubleWritable avg = new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            BigDecimal sum = BigDecimal.ZERO;
            BigDecimal count = BigDecimal.ZERO;
            for(DoubleWritable val: values){
                sum = sum.add(BigDecimal.valueOf(val.get()));
                count = count.add(BigDecimal.ONE);
            }
            if(!count.equals(BigDecimal.ONE)){
                avg.set(sum.divide(count,BigDecimal.ROUND_HALF_UP).doubleValue());
                context.write(key,new Text(avg.toString()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    static class SortMap extends Mapper<LongWritable,Text,PairKey,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs []= value.toString().split("\t+");
            if(strs.length!=2){
                return;
            }
            PairKey pairKey = new PairKey();
            pairKey.setId(new Text(strs[0].trim()));
            pairKey.setRate(new DoubleWritable(Double.parseDouble(strs[1].trim())));
            context.write(pairKey,NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    static class SortReduce extends Reducer<PairKey,NullWritable,Text,NullWritable>{
        private static final int K = 10;
        private static int count = 0;
        @Override
        protected void reduce(PairKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if(count<K){
                count++;
                context.write(new Text(key.toString()),NullWritable.get());
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    static class SortPartitioner extends Partitioner<PairKey,Text>{

        @Override
        public int getPartition(PairKey pairKey, Text text, int i) {
            return pairKey.getId().hashCode()%i;
        }
    }

    static class JoinMap1 extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs [] = value.toString().split("\t+");
            if(strs.length!=2){
                return;
            }
            Text bussid = new Text(strs[0].trim());
            Text other = new Text("sort::"+strs[1].trim());
            context.write(bussid,other);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    static class JoinMap2 extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs [] = value.toString().split("::");
            if(strs.length!=3){
                return;
            }
            Text bussid = new Text(strs[0].trim());
            Text other = new Text("join::"+strs[1].trim()+"\t"+strs[2].trim());
            context.write(bussid,other);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    static class JoinReduce extends Reducer<Text,Text,Text,Text>{
        private static String token1 = "sort::";
        private static String token2 = "join::";
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> buss = new LinkedList<>();
            List<Text> sort = new LinkedList<>();
            for(Text text:values){
                if(text.toString().startsWith(token1)){
                    Text newBuss = new Text();
                    newBuss.set(text.toString().substring(token1.length()));
                    if(!buss.contains(newBuss))
                        buss.add(newBuss);
                }  else if(text.toString().startsWith(token2)){
                    Text newSort = new Text();
                    newSort.set(text.toString().substring(token2.length()));
                    if(!sort.contains(newSort))
                        sort.add(newSort);
                }
            }

            for(Text b:buss){
                for(Text s: sort){
                    context.write(key,new Text(b.toString()+"\t"+s.toString()));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        String reviewPath = args[0];
        String businessPath = args[1];
        String output = args[2];

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf,"avg");
        job1.setJarByClass(Q3Class.class);
        job1.setMapperClass(AvgMap.class);
        job1.setReducerClass(AvgReduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job1,new Path(reviewPath));
        FileOutputFormat.setOutputPath(job1,new Path(output+"/average"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf,"sort");
        job2.setNumReduceTasks(1);
        job2.setReducerClass(SortReduce.class);
        job2.setMapperClass(SortMap.class);
        job2.setJarByClass(Q3Class.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setMapOutputKeyClass(PairKey.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setPartitionerClass(SortPartitioner.class);
        job2.setGroupingComparatorClass(MyComparator.class);
        FileInputFormat.addInputPath(job2,new Path(output+"/average/part-*"));
        FileOutputFormat.setOutputPath(job2,new Path(output+"/sort"));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf,"join");
        job3.setReducerClass(JoinReduce.class);
        MultipleInputs.addInputPath(job3,new Path(businessPath), TextInputFormat.class,JoinMap2.class);
        MultipleInputs.addInputPath(job3,new Path(output+"/sort/part-*"),TextInputFormat.class,JoinMap1.class);
        FileOutputFormat.setOutputPath(job3,new Path(output+"/join"));
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        System.exit(job3.waitForCompletion(true)?0:1);
    }
}

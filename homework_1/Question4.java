/**
 * Created by mac on 2/23/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class Question4 {
    public static class ReviewMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> bussId = new HashSet<String>();

        @SuppressWarnings("deprecation")
        protected void setup(Context context) throws java.io.IOException, InterruptedException {
            try {
                Configuration conf = context.getConfiguration();
                Path path = new Path (conf.get("filterFileURL"));
                FileSystem fs = FileSystem.get(conf);
                try {
                    BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String bussInfo = null;
                    while ((bussInfo = fis.readLine()) != null) {
                        String[] buss = bussInfo.split("::");
                        if (buss.length != 3) {
                            // illegal data
                            return;
                        }
                        if (buss[1].contains("Stanford")) {
                            bussId.add(buss[0]);
                        }
                    }
                } catch (IOException ioe) {
                    System.err.println("Exception while reading bussiness file '"
                            + path + "' : " + ioe.toString());
                }
            } catch (IOException e) {
                System.err.println("Exception reading cache file: " + e);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("::");
            if (split.length != 4) {
                // this record is in a wrong format, discard it
                return;
            }
            if (bussId.contains(split[2].trim())) {
                context.write(new Text(split[1].trim()), new Text(split[3].trim()));
//                context.write(new Text(split[1].trim()), new Text(split[3].trim() + "\t" + split[2]));
            }
        }

//        private void readCache(URI path) {
//            try {
//
//
//                BufferedReader fis = new BufferedReader(new FileReader(path.toString()));
//                String bussInfo = null;
//                while ((bussInfo = fis.readLine()) != null) {
//                    String[] buss = bussInfo.split("::");
//                    if (buss.length != 3) {
//                        // illegal data
//                        return;
//                    }
//                    if (buss[1].contains("Stanford")) {
//                        bussId.add(buss[0]);
//                    }
//                }
//            } catch (IOException ioe) {
//                System.err.println("Exception while reading bussiness file '"
//                        + path + "' : " + ioe.toString());
//            }
//        }
    }


    public static void main(String[] args) throws Exception {
        // usage: input/review.csv input/business.csv output/
        String reviewPath = args[0];
        String bussPath = args[1];
        String outputPath = args[2];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputFilePath = new Path(outputPath);
        if(fs.exists(outputFilePath)){
			   /*If exist delete the output path*/
            fs.delete(outputFilePath,true);
        }
        conf.set("filterFileURL", bussPath);
        Job job = Job.getInstance(conf, "Question4");
        job.setJarByClass(Question4.class);
        job.setMapperClass(ReviewMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(reviewPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Assignment4 {

    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            if (parts.length >= 3){
                String userId = parts[0].trim();
                String userAge = parts[2].trim();
                
                context.write(new Text(userId), new Text("A," + userAge));
            }
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            if (parts.length >= 3){
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                context.write(new Text(userId), new Text("R," + movieId + "," + rating));
            }
        }
    }

    private static String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-50";
        else return "50+";
    }

    public static class MiddleReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

            String age = null;
            List<String[]> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("A,")) {
                    age = v.substring(2);
                } else if (v.startsWith("R,")) {
                    ratings.add(v.substring(2).split(","));
                }
            }

            if (age != null) {
                int ageInt = Integer.parseInt(age);
                String group = getAgeGroup(ageInt);
                for (String[] r : ratings) {
                    context.write(new Text(r[0]),
                            new Text("G," + group + "," + r[1]));
                }   
            }
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            if (parts.length >= 3){
                String movieId = parts[0].trim();
                String movieTitle = parts[1].trim();

                context.write(new Text(movieId), new Text("T," + movieTitle));
            }
        }
    }

    public static class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Map<String, Double>> sumMap = new TreeMap<>();
        private Map<String, Map<String, Integer>> countMap = new TreeMap<>();
        private Map<String, String> titleMap = new HashMap<>();
        private Map<String, String> info = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            String movieId = key.toString();

            for (Text val: values){
                String v = val.toString();
                if (v.startsWith("T,")){
                    titleMap.put(movieId, v.substring(2));
                }
                else if (v.startsWith("G,")){
                    String[] parts = v.split(",");
                    String group = parts[1];
                    double rating = Double.parseDouble(parts[2]);

                    sumMap.putIfAbsent(movieId, new HashMap<>());
                    countMap.putIfAbsent(movieId, new HashMap<>());

                    Map<String, Double> sMap = sumMap.get(movieId);
                    Map<String, Integer> cMap = countMap.get(movieId);

                    sMap.put(group, sMap.getOrDefault(group, 0.0) + rating);
                    cMap.put(group, cMap.getOrDefault(group, 0) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String movieId : sumMap.keySet()) {

                String title = titleMap.getOrDefault(movieId, movieId);

                Map<String, Double> sMap = sumMap.get(movieId);
                Map<String, Integer> cMap = countMap.get(movieId);

                String[] groups = {"0-18", "18-35", "35-50", "50+"};

                StringBuilder result = new StringBuilder();

                for (String g : groups) {
                    double avg = 0.0;

                    if (cMap.containsKey(g)) {
                        avg = sMap.get(g) / cMap.get(g);
                        result.append(g).append(": ")
                          .append(String.format("%.2f", avg)).append(", ");
                    }
                    else {
                        result.append(g).append(": ")
                          .append("NA").append(", ");
                    }
                }
                info.put(title, result.toString());
            }
            for (Map.Entry<String, String> entry: info.entrySet()){
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // ===== JOB 1 =====
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "Join Ratings and Users");

        job1.setJarByClass(Assignment4.class);

        MultipleInputs.addInputPath(job1, new Path(args[0] + "/ratings_1.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job1, new Path(args[0] + "/ratings_2.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job1, new Path(args[0] + "/users.txt"),
                TextInputFormat.class, UserMapper.class);

        job1.setReducerClass(MiddleReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path tempOutput = new Path(args[0] + "/temp_output");
        FileSystem fs = FileSystem.get(conf1);

        if (fs.exists(tempOutput)) {
            fs.delete(tempOutput, true); // true = xóa đệ quy
        }

        FileOutputFormat.setOutputPath(job1, tempOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ===== JOB 2 =====
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join Movies and Compute Avg");

        job2.setJarByClass(Assignment4.class);

        // input 1: output job1
        MultipleInputs.addInputPath(job2, tempOutput,
                TextInputFormat.class, IdentityMapper.class);

        // input 2: movies
        MultipleInputs.addInputPath(job2, new Path(args[0] + "/movies.txt"),
                TextInputFormat.class, MovieMapper.class);

        job2.setReducerClass(FinalReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

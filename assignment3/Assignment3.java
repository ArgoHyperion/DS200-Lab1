import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Assignment3 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                context.write(new Text(userId), new Text("R|" + movieId + "|" + rating));
            }
        }
    }

    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String gender = parts[1].trim();

                context.write(new Text(userId), new Text("G|" + gender));
            }
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                String movieId = parts[0].trim();
                String movieTitle = parts[1].trim();

                context.write(new Text(movieId), new Text("T|" + movieTitle));
            }
        }
    }

    public static class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    public static class MovieIdGenderRatingReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

            String gender = null;
            List<String[]> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("G|")) {
                    gender = v.substring(2);
                } else if (v.startsWith("R|")) {
                    ratings.add(v.substring(2).split("\\|"));
                }
            }

            if (gender != null) {
                for (String[] r : ratings) {
                    String movieId = r[0];
                    String rating = r[1];
                    context.write(new Text(movieId),
                            new Text("G|" + gender + "|" + rating));
                }
            }
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Double> maleSum = new TreeMap<>();
        private Map<String, Integer> maleCount = new TreeMap<>();

        private Map<String, Double> femaleSum = new TreeMap<>();
        private Map<String, Integer> femaleCount = new TreeMap<>();

        Map<String, String> resultMap = new TreeMap<>();
        Map<String, String> movieTitleMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = null;

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("T|")) {
                    movieTitle = v.substring(2);
                } else if (v.startsWith("G|")) {

                    String[] parts = v.split("\\|");

                    String gender = parts[1];
                    double rating = Double.parseDouble(parts[2]);

                    if (gender.equals("M")) {
                        maleSum.put(key.toString(),
                            maleSum.getOrDefault(key.toString(), 0.0) + rating);
                        maleCount.put(key.toString(),
                            maleCount.getOrDefault(key.toString(), 0) + 1);
                    } else if (gender.equals("F")) {
                        femaleSum.put(key.toString(),
                            femaleSum.getOrDefault(key.toString(), 0.0) + rating);
                        femaleCount.put(key.toString(),
                            femaleCount.getOrDefault(key.toString(), 0) + 1);
                    }
                }
            }

            if (movieTitle != null) {
                movieTitleMap.put(key.toString(), movieTitle);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            Set<String> movies = new TreeSet<>();
            movies.addAll(maleSum.keySet());
            movies.addAll(femaleSum.keySet());

            for (String movieId : movies) {

                String title = movieTitleMap.getOrDefault(movieId, movieId);

                double mAvg = maleCount.containsKey(movieId)
                        ? maleSum.get(movieId) / maleCount.get(movieId) : 0;

                double fAvg = femaleCount.containsKey(movieId)
                        ? femaleSum.get(movieId) / femaleCount.get(movieId) : 0;

                String result = String.format("Male: %.2f, Female: %.2f", mAvg, fAvg);
                resultMap.put(title, result);
            }

            for (String title : resultMap.keySet()) {
                context.write(new Text(title), new Text(resultMap.get(title)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // ===== JOB 1 =====
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Join Ratings and Users");

        job1.setJarByClass(Assignment3.class);

        MultipleInputs.addInputPath(job1, new Path(args[0] + "/ratings_1.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job1, new Path(args[0] + "/ratings_2.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job1, new Path(args[0] + "/users.txt"),
                TextInputFormat.class, UserMapper.class);

        job1.setReducerClass(MovieIdGenderRatingReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path tempOutput = new Path(args[0] + "/temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ===== JOB 2 =====
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join Movies and Compute Avg");

        job2.setJarByClass(Assignment3.class);

        // input 1: output job1
        MultipleInputs.addInputPath(job2, tempOutput,
                TextInputFormat.class, IdentityMapper.class); // identity mapper

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

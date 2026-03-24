import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Assignment2 {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                String movieId = parts[0].trim();
                String genres = parts[2].trim();

                context.write(new Text(movieId), new Text("M|" + genres));
            }
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                context.write(new Text(movieId), new Text("R|" + rating));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private Map<String, Double> sumMap = new TreeMap<>();
        private Map<String, Integer> countMap = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Double> ratings = new ArrayList<>();
            String[] genres = null;

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("M|")) {
                    genres = v.substring(2).split("\\|");
                } else if (v.startsWith("R|")) {
                    ratings.add(Double.parseDouble(v.substring(2)));
                }
            }

            if (genres != null && ratings.size() > 0) {
                for (String genre : genres) {
                    genre = genre.trim();

                    for (double r : ratings) {
                        sumMap.put(genre, sumMap.getOrDefault(genre, 0.0) + r);
                        countMap.put(genre, countMap.getOrDefault(genre, 0) + 1);
                    }
                }
            }
        }
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String genre : sumMap.keySet()) {
                double sum = sumMap.get(genre);
                int count = countMap.get(genre);
                double avg = sum / count;

                String result = String.format("Avg: %.2f, Count: %d", avg, count);
                context.write(new Text(genre), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");

        job.setJarByClass(Assignment2.class);

        MultipleInputs.addInputPath(job, new Path(args[0] + "/movies.txt"),
                TextInputFormat.class, MovieMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[0] + "/ratings_1.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[0] + "/ratings_2.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[0] + "/users.txt"),
                TextInputFormat.class, RatingMapper.class);

        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

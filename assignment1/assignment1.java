import java.io.IOException;
import java.util.Map;
import java.util.TreeMap; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Assignment1 {

    // ================= TITLE MAPPER =================
    public static class TitleMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // MovieID,Title,Genres
            String[] parts = value.toString().split(",");

            if (parts.length >= 2) {
                String movieId = parts[0].trim();
                String title = parts[1].trim();

                context.write(new Text(movieId), new Text("T|" + title));
            }
        }
    }

    // ================= RATING MAPPER =================
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // UserID,MovieID,Rating,Timestamp
            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                context.write(new Text(movieId), new Text("R|" + rating));
            }
        }
    }

    // ================= REDUCER =================
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private Map<String, String> resultMap = new TreeMap<>();
        private String maxMovie = "";
        private double maxRating = Double.MIN_VALUE;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title = "";
            double sum = 0;
            int count = 0;

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("T|")) {
                    title = v.substring(2);
                } else if (v.startsWith("R|")) {
                    double rating = Double.parseDouble(v.substring(2).trim());
                    sum += rating;
                    count++;
                }
            }

            if (!title.isEmpty() && count > 0) {
                double avg = sum / count;

                String info = String.format("AverageRating: %.2f (Total ratings: %d)", avg, count);
                
                resultMap.put(title, info);

                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = title;
                }
            }
        }
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Map.Entry<String, String> entry : resultMap.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }

            if (!maxMovie.isEmpty()) {
                context.write(new Text("RESULT"),
                    new Text(maxMovie + " is the highest rated movie with an average rating of "
                            + maxRating + " among movies with at least 5 ratings."));
            }
        }
    }

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Join");

        job.setJarByClass(MovieRatingJoin.class);

        // Multiple input
        MultipleInputs.addInputPath(job, new Path(args[0] + "/movies.txt"),
                TextInputFormat.class, TitleMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[0] + "/ratings_1.txt"),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[0] + "/ratings_2.txt"),
                TextInputFormat.class, RatingMapper.class);

        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

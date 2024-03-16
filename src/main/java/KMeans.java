import java.io.IOException;
import java.util.StringTokenizer;

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

import models.Point;
import models.PointAggregator;

public class KMeans {
    // Mapper class for KMeans. It should read the centroids from the context and emit the closest centroid for each point.
    public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        private Point[] centroids = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            centroids = new Point[k];
            for (int i = 0; i < k; i++) {
                centroids[i] = new Point(conf.get("c" + i));
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Point p = new Point(value.toString());
            int closestCentroid = 0;
            float minDistance = Float.MAX_VALUE;
            for (int i = 0; i < centroids.length; i++) {
                float distance = p.distance(centroids[i]);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = i;
                }
            }
            context.write(new IntWritable(closestCentroid), p);
        }
    }

    public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
        @Override
        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            PointAggregator aggregator = new PointAggregator();
            for (Point p : values) {
                aggregator.aggregate(p);
            }
            aggregator.average();
            context.write(key, aggregator.toPoint());
        }
    }

    public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            PointAggregator aggregator = new PointAggregator();
            for (Point p : values) {
                aggregator.aggregate(p);
            }
            aggregator.average();
            context.write(new Text("c" + key.toString()), new Text(aggregator.toPoint().toString()));
        }
    }

    private static Point[] generateRandomCentroids(int k, int dimensions) {
        Point[] centroids = new Point[k];
        for (int i = 0; i < k; i++) {
            centroids[i] = Point.randomPoint(dimensions);
        }
        return centroids;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: KMeans <input path> <output path> <k> <dimensions>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[2]);
        int dimensions = Integer.parseInt(args[3]);
        conf.set("k", args[2]);
        conf.set("dimensions", args[3]);
        
        Point[] centroids = generateRandomCentroids(k, dimensions);
        for (int i = 0; i < k; i++) {
            conf.set("c" + i, centroids[i].toString());
        }

        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Point.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

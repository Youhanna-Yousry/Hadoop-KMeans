import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointAggregator> {
        private Point[] centroids = null;

        @Override
        protected void setup(Context context) {
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
            context.write(new IntWritable(closestCentroid), new PointAggregator(p));
        }
    }

    public static class KMeansCombiner extends Reducer<IntWritable, PointAggregator, IntWritable, PointAggregator> {
        @Override
        public void reduce(IntWritable key, Iterable<PointAggregator> values, Context context) throws IOException, InterruptedException {
            PointAggregator aggregator = new PointAggregator();
            for (PointAggregator p : values) {
                aggregator.aggregate(p);
            }
            context.write(key, aggregator);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, PointAggregator, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<PointAggregator> values, Context context) throws IOException, InterruptedException {
            PointAggregator aggregator = new PointAggregator();
            for (PointAggregator p : values) {
                aggregator.aggregate(p);
            }
            aggregator.average();
            context.write(new Text(key.toString()), new Text(aggregator.toPoint().toString()));
        }
    }

    // Select a random point from the input data as the initial centroid
    private static Point[] chooseRandomCentroids(Configuration conf, int k, String pathString){
        Point[] points = new Point[k];
        FileSystem hdfs;
        try {
            hdfs = FileSystem.get(conf);
            FileStatus[] status = hdfs.listStatus(new Path(pathString));
            Set<String> lines = new HashSet<String>();
            for (FileStatus fileStatus : status) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
                br.close();
            }
            for (int i = 0; i < k; i++) {
                int random = (int) (Math.random() * lines.size());
                int j = 0;
                for (String line : lines) {
                    if (j == random) {
                        points[i] = new Point(line);
                        break;
                    }
                    j++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return points;
    }
    
    private static boolean hasConverged(Point[] oldCentroids, Point[] newCentroids, float threshold) {
        for (int i = 0; i < oldCentroids.length; i++) {
            if (oldCentroids[i].distance(newCentroids[i]) > threshold) {
                return false;
            }
        }
        return true;
    }

    private static Point[] readCentroids(Configuration conf, int k, String pathString) throws IOException, FileNotFoundException {
        Point[] points = new Point[k];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (FileStatus fileStatus : status) {
            //Read the centroids from the hdfs
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));
                String line;
                while ((line = br.readLine()) != null){
                    String[] keyValueSplit = line.split("\t"); //Split line in K,V
                    int centroidId = Integer.parseInt(keyValueSplit[0]);
                    points[centroidId] = new Point(keyValueSplit[1]);
                }
                br.close();
            }
        }
        //Delete temp directory
        hdfs.delete(new Path(pathString), true); 

    	return points;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: KMeans <input path> <output path> <k> <max iterations>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);
        conf.set("k", args[2]);
        
        Point[] centroids = chooseRandomCentroids(conf, k, args[0]);
        for (int i = 0; i < k; i++) {
            conf.set("c" + i, centroids[i].toString());
            System.out.println("c" + i + " = " + centroids[i].toString());
        }
        
        boolean converged = false;
        int iteration = 0;
        while(!converged){
            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(PointAggregator.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(PointAggregator.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean success = job.waitForCompletion(true);
            
            if(!success){
                System.err.println("Error running iteration " + iteration);
                System.exit(1);
            }
            
            if (iteration >= maxIterations) {
                System.out.println("Max iterations reached");
                break;
            }
            
            Point[] oldCentroids = centroids;
            centroids = readCentroids(conf, k, args[1]);
            converged = hasConverged(oldCentroids, centroids, 1e-6f);

            if(converged){
                System.out.println("Converged after " + iteration + " iterations");
                for (int i = 0; i < k; i++) {
                    System.out.println("c" + i + " = " + centroids[i].toString());
                }
            }
            else{
                for (int i = 0; i < k; i++) {
                    conf.unset("c" + i);
                    conf.set("c" + i, centroids[i].toString()); 
                }
            }
            iteration++;
        }        
    }
}

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import models.Point;
import models.PointAggregator;

import kmeans.KMeansCombiner;
import kmeans.KMeansMapper;
import kmeans.KMeansReducer;

import helpers.RandomPicker;
import helpers.HDFSReader;

public class KMeans {
    // Select a random point from the input data as the initial centroid
    private static Point[] chooseRandomCentroids(String inputPath, int k){
        Point[] points = new Point[k];
        RandomPicker picker = new RandomPicker();
        try {
            String[] pickedLines = picker.pickRandom(inputPath, k);
            for (int i = 0; i < k; i++) {
                points[i] = new Point(pickedLines[i]);
            }
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
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
        String[] output = HDFSReader.read(conf, pathString);
        for(int i = 0; i < k; i++) {
            output[i] = output[i].split("\t")[1];
            points[i] = new Point(output[i]);
        }
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(new Path(pathString), true);
        return points;
    }

    private static void setCentroids(Configuration conf, Point[] centroids) {
        for (int i = 0; i < centroids.length; i++) {
            conf.unset("c" + i);
            conf.set("c" + i, centroids[i].toString());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: KMeans <input path> <output path> <k> <max iterations>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];
        int k = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);
        conf.set("k", args[2]);
        
        Point[] centroids = chooseRandomCentroids(inputPath, k);
        setCentroids(conf, centroids);
        
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
            centroids = readCentroids(conf, k, outputPath);
            converged = hasConverged(oldCentroids, centroids, 1e-6f);

            if(converged){
                System.out.println("Converged after " + iteration + " iterations");
                for (int i = 0; i < k; i++) {
                    System.out.println("c" + i + " = " + centroids[i].toString());
                }
            }
            else{
                setCentroids(conf, centroids);
            }
            iteration++;
        }        
    }
}

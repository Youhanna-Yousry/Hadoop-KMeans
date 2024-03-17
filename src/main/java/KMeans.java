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

/**
 * The KMeans class is the main class for the KMeans algorithm. It chooses random centroids, runs the KMeans algorithm,
 * and outputs the final centroids.
 */
public class KMeans {
    /**
     * Chooses random centroids from the input file.
     * @param inputPath The input file
     * @param k The number of centroids to choose
     * @return An array of k points chosen at random
     */
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

    /**
     * Checks if the centroids have converged.
     * @param oldCentroids The old centroids
     * @param newCentroids The new centroids
     * @param threshold The threshold for convergence
     * @return True if the centroids have converged, false otherwise
     */
    private static boolean hasConverged(Point[] oldCentroids, Point[] newCentroids, float threshold) {
        for (int i = 0; i < oldCentroids.length; i++) {
            if (oldCentroids[i].distance(newCentroids[i]) > threshold) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the centroids from the output of the KMeansReducer.
     * @param conf The configuration of the job
     * @param k The number of centroids
     * @param pathString The path to the output file
     * @return An array of k points representing the centroids
     */
    private static Point[] readCentroids(Configuration conf, int k, String pathString) throws IOException {
        Point[] points = new Point[k];
        String[] output = HDFSReader.read(conf, pathString);
        int i;
        for(i = 0; i < k && i < output.length; i++) {
            output[i] = output[i].split("\t")[1];
            points[i] = new Point(output[i]);
        }
        while (i < k) {
            points[i] = Point.randomPoint(points[0].getValues().length);
            i++;
        }
        FileSystem hdfs = FileSystem.get(conf);
        // Delete the output directory
        hdfs.delete(new Path(pathString), true);
        return points;
    }

    /**
     * Sets the centroids in the configuration.
     * @param conf The configuration of the job
     * @param centroids The centroids to set
     */
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

        // Setup
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

            // Job failed
            if(!success){
                System.err.println("Error running iteration " + iteration);
                System.exit(1);
            }

            // Max iterations reached
            if (iteration >= maxIterations) {
                System.out.println("Max iterations reached");
                break;
            }

            // Check for convergence
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

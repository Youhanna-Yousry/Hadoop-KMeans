package kmeans;

import models.Point;
import models.PointAggregator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The KMeansMapper class is the mapper for the KMeans algorithm. It takes a set of points and a set of centroids and
 * assigns each point to the closest centroid. It emits the index of the closest centroid as the key and the point as
 * the value.
 */
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointAggregator> {
    private Point[] centroids = null;

    /**
     * The setup function reads the centroids from the configuration and stores them in the centroids array.
     * @param context The context of the job
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("k"));

        centroids = new Point[k];
        for (int i = 0; i < k; i++) {
            centroids[i] = new Point(conf.get("c" + i));
        }
    }

    /**
     * The map function takes a point and assigns it to the closest centroid. It emits the index of the closest centroid
     * as the key and the point as the value.
     * @param key The line number of the input file
     * @param value The point as a string
     * @param context The context of the job
     */
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
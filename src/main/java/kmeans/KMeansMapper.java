package kmeans;

import models.Point;
import models.PointAggregator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointAggregator> {
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
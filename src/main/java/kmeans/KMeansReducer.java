package kmeans;

import models.PointAggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The KMeansReducer class is the reducer for the KMeans algorithm. It takes a set of aggregated points and averages
 * their values to compute the new centroids. It emits the index of the centroid as the key and the new centroid as the
 * value.
 */
public class KMeansReducer extends Reducer<IntWritable, PointAggregator, Text, Text> {

    /** The reduce function takes a set of aggregated points and averages their values to compute the new centroids. It
     * emits the index of the centroid as the key and the new centroid as the value.
     * @param key The index of the centroid
     * @param values The set of aggregated points
     * @param context The context of the job
     */
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
package kmeans;

import models.PointAggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The KMeansCombiner class is the combiner for the KMeans algorithm. It takes a set of points and aggregates them by
 * summing their values and counting the number of points. It emits the index of the centroid as the key and the
 * aggregated point as the value.
 */
public class KMeansCombiner extends Reducer<IntWritable, PointAggregator, IntWritable, PointAggregator> {

    /** The reduce function takes a set of points and aggregates them by summing their values and counting the number of
     * points. It emits the index of the centroid as the key and the aggregated point as the value.
     * @param key The index of the centroid
     * @param values The set of points
     * @param context The context of the job
     */
    @Override
    public void reduce(IntWritable key, Iterable<PointAggregator> values, Context context) throws IOException, InterruptedException {
        PointAggregator aggregator = new PointAggregator();
        for (PointAggregator p : values) {
            aggregator.aggregate(p);
        }
        context.write(key, aggregator);
    }
}
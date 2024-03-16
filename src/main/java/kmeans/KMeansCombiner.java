package kmeans;

import models.PointAggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, PointAggregator, IntWritable, PointAggregator> {
    @Override
    public void reduce(IntWritable key, Iterable<PointAggregator> values, Context context) throws IOException, InterruptedException {
        PointAggregator aggregator = new PointAggregator();
        for (PointAggregator p : values) {
            aggregator.aggregate(p);
        }
        context.write(key, aggregator);
    }
}
package kmeans;

import models.PointAggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, PointAggregator, Text, Text> {
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
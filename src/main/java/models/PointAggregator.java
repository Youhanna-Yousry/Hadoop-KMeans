package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointAggregator implements Writable {
    private float[] values = null;
    private int numPoints = 0;

    public PointAggregator(){}

    public PointAggregator(final int numDimensions){
        this.values = new float[numDimensions];
        numPoints = 1;
    }

    public PointAggregator(final String str) {
        String[] parts = str.split(",");
        this.values = new float[parts.length - 1];
        for (int i = 0; i < this.values.length; i++) {
            this.values[i] = Float.parseFloat(parts[i]);
        }
        this.numPoints = Integer.parseInt(parts[parts.length - 1]);
    }

    public PointAggregator(Point p){
        this.values = p.getValues();
        this.numPoints = 1;
    }

    public void aggregate(final Point p) {
        if (this.values == null) {
            this.values = p.getValues();
            this.numPoints = 1;
        }
        else {
            float[] pValues = p.getValues();
            for (int i = 0; i < this.values.length; i++) {
                this.values[i] += pValues[i];
            }
            this.numPoints += 1;
        }
    }

    public void aggregate(final PointAggregator p) {
        if (this.values == null) {
            this.values = p.values;
            this.numPoints = p.numPoints;
        }
        else {
            for (int i = 0; i < this.values.length; i++) {
                this.values[i] += p.values[i];
            }
            this.numPoints += p.numPoints;
        }
    }

    public void average() {
        for (int i = 0; i < this.values.length; i++) {
            this.values[i] /= this.numPoints;
        }
        numPoints = 1;
    }

    public Point toPoint() {
        return new Point(this.values);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < this.values.length; i++) {
            strBuilder.append(Float.toString(this.values[i]));
            strBuilder.append(",");
        }
        strBuilder.append(this.numPoints);
        return strBuilder.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.numPoints = in.readInt();
        int numValues = in.readInt();
        this.values = new float[numValues];
        for (int i = 0; i < numValues; i++) {
            this.values[i] = in.readFloat();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numPoints);
        out.writeInt(this.values.length);
        for (int i = 0; i < this.values.length; i++) {
            out.writeFloat(this.values[i]);
        } 
    }  
}

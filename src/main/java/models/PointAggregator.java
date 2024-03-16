package models;

public class PointAggregator {
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
}

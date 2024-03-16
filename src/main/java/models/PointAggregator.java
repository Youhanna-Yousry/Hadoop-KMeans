package models;

public class PointAggregator {
    private float[] values = null;
    private int numPoints = 0;

    public PointAggregator(){}

    public PointAggregator(final int numDimensions){
        this.values = new float[numDimensions];
        numPoints = 1;
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

    public void average() {
        for (int i = 0; i < this.values.length; i++) {
            this.values[i] /= this.numPoints;
        }
    }

    public Point toPoint() {
        return new Point(this.values);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < this.values.length; i++) {
            strBuilder.append(Float.toString(this.values[i]));
            if(i != this.values.length - 1) {
                strBuilder.append(",");
            }
        }
        strBuilder.append(" - numPoints: ");
        strBuilder.append(this.numPoints);
        return strBuilder.toString();
    }
}

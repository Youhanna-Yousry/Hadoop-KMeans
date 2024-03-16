package models;

public class Point {
    private float[] values = null;

    public Point(){}

    public Point(final float[] coordinates){
        this.values = coordinates;
    }

    public Point(final String line){
        String[] coordinates = line.split(",");
        this.values = new float[coordinates.length];
        for (int i = 0; i < coordinates.length; i++){
            this.values[i] = Float.parseFloat(coordinates[i]);
        }
    }

    public final float[] getValues() {
        return this.values;
    }

    public static Point randomPoint(int dimensions){
        float[] values = new float[dimensions];
        float range = Float.MAX_VALUE - Float.MIN_VALUE;
        for (int i = 0; i < dimensions; i++){
            values[i] = (float) (Math.random() * range) + Float.MIN_VALUE;
        }
        return new Point(values);
    }

    /**
     * Euclidean distance between two points
     * @param p The point to calculate the distance to
     * @return The distance between the two points
     */
    public float distance(Point p) {
        float sum = 0;
        for (int i = 0; i < this.values.length; i++) {
            sum += Math.pow(this.values[i] - p.values[i], 2);
        }
        return (float) Math.sqrt(sum);
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.values.length; i++) {
            point.append(Float.toString(this.values[i]));
            if(i != this.values.length - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }
}

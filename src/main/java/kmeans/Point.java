package kmeans;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Point {

    private int id;
    private List<Double> vector;

    Point() {
        this.id = -1;
        vector = new ArrayList<>();
    }

    public static Point  parsePoint(String raw) {
            raw = raw.trim();
            String[] strs = raw.split("\\s+");
            Point p =new Point();
            p.setId(Integer.parseInt(strs[0]));
            for(int i = 1; i < strs.length; i++) {
                p.getVector().add(Double.parseDouble(strs[i]));
            }
            return p;
    }

    //欧几里得距离
    public double euclideanDistance(Point other) {
        double sum = 0;
        for(int i = 0; i < this.vector.size(); i++) {
            double temp = this.vector.get(i) - other.getVector().get(i);
            sum += temp*temp;
        }
        return Math.sqrt(sum);
    }

    public Point add(Point other) {
        Point p = new Point();
        for(int i = 0; i < this.vector.size(); i++) {
            double temp = this.vector.get(i) + other.getVector().get(i);
            p.getVector().add(temp);
        }
        return p;
    }
}

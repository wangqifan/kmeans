package kmeans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

public class CentroidAverager implements MapFunction<Tuple3<Point, Point, Long>, Point> {
    @Override
    public Point map(Tuple3<Point, Point, Long> value) throws Exception {
        Point p = new Point();
        List<Double> sum = value.f1.getVector();
        for(int i = 0; i < sum.size(); i++) {
            p.getVector().add(sum.get(i)/value.f2);
        }
        return p;
    }
}

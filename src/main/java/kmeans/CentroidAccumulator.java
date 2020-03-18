package kmeans;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CentroidAccumulator implements ReduceFunction<Tuple3<Point,Point,Long>> {
    @Override
    public Tuple3<Point, Point, Long> reduce(Tuple3<Point, Point, Long> value, Tuple3<Point, Point, Long> t1) throws Exception {
        return new Tuple3<>(value.f0, value.f1.add(t1.f1), value.f2 + t1.f2);
    }
}

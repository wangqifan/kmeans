package kmeans;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CountAppender implements MapFunction<Tuple2<Point,Point>, Tuple3<Point,Point,Long>> {
    @Override
    public Tuple3<Point, Point, Long> map(Tuple2<Point, Point> value) throws Exception {
        return new Tuple3<>(value.f0, value.f1, 1L);
    }
}

package kmeans;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

/** Determines the closest cluster center for a data point.
 * 找到最近的聚类中心
 * */

public class SelectNearestCenter extends RichFlatMapFunction<Point, Tuple2<Point,Point>> {

    private Collection<Point> centroids;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public void flatMap(Point point, Collector<Tuple2<Point, Point>> out) throws Exception {
        double minDistance = Double.MAX_VALUE;
        Point closestCentro = null;

        //检查所有聚类中心
        for(Point centroid : centroids){
            //计算点到聚类中心的距离
            double distance = point.euclideanDistance(centroid);

            //更新最小距离
            if(distance<minDistance){
                minDistance = distance;
                closestCentro = centroid;
            }
        }
        out.collect(new Tuple2<>(closestCentro,point));
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kmeans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读csv文件
		String path = BatchJob.class.getClassLoader().getResource("color100.txt").getPath();
	    DataSet<String> dataSet = env.readTextFile(path);

	    // 解析 转化为point对象
		DataSet<Point> points =dataSet
				.map( str -> Point.parsePoint(str));

        // 获取前四个作为默认的聚类中心
		DataSet<Point> centroid = points.first(4);

		// 设置迭代
		IterativeDataSet<Point> loop = centroid.iterate(10);

       DataSet<Point> newCentor =
		    points
					.flatMap(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
					.map(new CountAppender())
					.groupBy(value -> value.f0.hashCode())
					.reduce(new CentroidAccumulator())
					.map(new CentroidAverager());

		DataSet<Point> finalCentroid = loop.closeWith(newCentor);

		finalCentroid.print();

		// execute program
		env.execute("kmeans");
	}
}

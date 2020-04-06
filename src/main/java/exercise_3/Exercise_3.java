package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.*;

public class Exercise_3 {

    private static class VProg extends
            AbstractFunction3<Long,Tuple2,Tuple2,Tuple2> implements Serializable {
        @Override
        public Tuple2 apply(Long vertexID, Tuple2 vertexValue, Tuple2 message) {
            if(((Integer) vertexValue._1) < ((Integer) message._1)) {
                return vertexValue; }
            else { return message; }
        }
    }

    private static class sendMsg extends
            AbstractFunction1<EdgeTriplet<Tuple2<Integer,ArrayList<Long>>,Integer>,
                    Iterator<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>>
            apply(EdgeTriplet<Tuple2<Integer,ArrayList<Long>>,Integer> triplet) {

            Tuple2<Object,Tuple2<Integer,ArrayList<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Tuple2<Integer,ArrayList<Long>>> dstVertex = triplet.toTuple()._2();
            Integer edge = triplet.toTuple()._3();
//            ArrayList<Long> al = new ArrayList<Long>() {{
//                addAll(sourceVertex._2()._2);
//                add(triplet.dstId());
//            }};

            if (sourceVertex._2()._1 != Integer.MAX_VALUE && dstVertex._2()._1 > (sourceVertex._2()._1 + edge)) {
                // propagate source vertex value
                return JavaConverters.asScalaIteratorConverter(
                        Arrays.asList(new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>
                                (triplet.dstId(), new Tuple2<Integer,ArrayList<Long>> (sourceVertex._2()._1+edge,
                                        new ArrayList<Long>() {{ addAll(sourceVertex._2()._2); add(triplet.dstId());}}
                                        ))).iterator()).asScala();
            } else {
                // do nothing
                return JavaConverters.asScalaIteratorConverter(
                        new ArrayList<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>>().iterator()).asScala();
            }
        }
    }

    private static class merge extends
            AbstractFunction2<Tuple2,Tuple2,Tuple2> implements Serializable {
        @Override
        public Tuple2 apply(Tuple2 o, Tuple2 o2) {
            if(((Integer) o._1) < ((Integer) o2._1)) {
                return o; }
            else { return o2; }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

//        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
//                new Tuple2<Object,Integer>(1l,0),
//                new Tuple2<Object,Integer>(2l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(3l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(4l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(5l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(6l,Integer.MAX_VALUE)
//        );

        List<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>> vertices_test = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>(1l,
                        new Tuple2<Integer,ArrayList<Long>>(0, new ArrayList<Long>() {{ add(1l); }} )),
                new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>(2l,
                        new Tuple2<Integer,ArrayList<Long>>(Integer.MAX_VALUE, new ArrayList<Long>() {{ add(1l); }} )),
                new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>(3l,
                        new Tuple2<Integer,ArrayList<Long>>(Integer.MAX_VALUE, new ArrayList<Long>() {{ add(1l); }} )),
                new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>(4l,
                        new Tuple2<Integer,ArrayList<Long>>(Integer.MAX_VALUE, new ArrayList<Long>() {{ add(1l); }} )),
                new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>(5l,
                        new Tuple2<Integer,ArrayList<Long>>(Integer.MAX_VALUE, new ArrayList<Long>() {{ add(1l); }} )),
                new Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>(6l,
                        new Tuple2<Integer,ArrayList<Long>>(Integer.MAX_VALUE, new ArrayList<Long>() {{ add(1l); }} ))
        );


        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

//        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>> verticesRDD_test = ctx.parallelize(vertices_test);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer,ArrayList<Long>>,Integer> G = Graph.apply(verticesRDD_test.rdd(),edgesRDD.rdd(),
                new Tuple2<Integer,ArrayList<Long>>(1, new ArrayList<Long>()),
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(
                new Tuple2(Integer.MAX_VALUE, new ArrayList<Long>()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .sortBy(f -> ((Tuple2<Object, Tuple2<Integer,ArrayList<Long>>>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Tuple2<Integer,ArrayList<Long>>> vertex =
                            (Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>)v;

                    //Change the vertex ID into alphabetical letters for the path
                    ArrayList<String> path = new ArrayList<String>();
                    for (Long num : vertex._2()._2) {
                        path.add(labels.get(num));
                    }

                    System.out.println("Minimum path to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "
                            +path+" with cost "+vertex._2()._1);
                });
    }
}

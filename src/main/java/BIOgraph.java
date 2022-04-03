import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import ppiscala.graphAlgorithms;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BIOgraph {
    private JavaSparkContext jsc;
    public  GraphFrame ppi;

    public BIOgraph(JavaSparkContext jsc, GraphFrame ppi){
        this.jsc=jsc;
        this.ppi=ppi;
        jsc.sc().setCheckpointDir("PPI-Check");
    }

    public Dataset<Row> interactors(){
        return ppi.vertices();
    }
    public long interactorCounter(){
        return ppi.vertices().count();
    }
    public Dataset<Row> interactions(){return ppi.edges();}
    public long interactionsCounter(){
        return ppi.edges().count();
    }


    public double density(){
        double nVertices=graph.vertices().count();
        double nEdges=graph.edges().count();
        return  (2*nEdges)/(nVertices*(nVertices-1));
    }


    public void degrees(int limit){
        ppi.degrees().orderBy(functions.col("degree").desc()).limit(20).coalesce(1).write().csv("degree");
    }

    public Dataset<Row> closeness(ArrayList<Object> landmarks){
        Dataset<Row> paths=graph.shortestPaths().landmarks(landmarks).run();
        Dataset<Row> explodedPaths=paths
                .select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")));
        Dataset<Row> t=explodedPaths.groupBy("key").sum("value");
        return 	t.withColumn("sum(value)",org.apache.spark.sql.functions.pow(t.col("sum(value)"),-1));
    }

    public  Dataset<Row> xNeighbors(String protein_name, int x_threshold){
        ArrayList<Object> proteins=new ArrayList<Object>();
        proteins.add(protein_name);
        Dataset<Row> neighborhood=ppi.shortestPaths().landmarks(proteins).run();
        Dataset<Row> x_neighborhood=neighborhood
                .select(neighborhood.col("id"),functions.explode(neighborhood.col("distances")))
                .filter("value<="+x_threshold)
                .drop("key")
                .drop("value");
        return x_neighborhood;
    }

    public GraphFrame xSubGraph(ArrayList<Object> N,int x){
        System.out.println("edges");
        ppi.edges().show();
        Dataset<Row> paths=ppi.shortestPaths().landmarks(N).run();

        Dataset<Row> explodedPaths=paths
                    .select(paths.col("id"),org.apache.spark.sql.functions.explode(paths.col("distances")))
                    .filter("value<="+x)
                    .drop("value")
                    .groupBy("id")
                    .agg(org.apache.spark.sql.functions.collect_list("key").as("neighbors"));

        Dataset<Row> edges=ppi.edges()
                    .join(explodedPaths,ppi.edges().col("src").equalTo(explodedPaths.col("id")));
        edges=edges.drop("id","neighbors");
        edges=edges.join(explodedPaths,edges.col("dst").equalTo(explodedPaths.col("id")));
        edges=edges.drop("id","neighbors");

        return GraphFrame.fromEdges(edges);
    }

    public BIOgraph intersectionNetwork(String[] sourceNode){

        return new BIOgraph(jsc,ppi);
    }
    public Dataset<Row> shortestPath(){
        return null;
    }
    public void x_weighted_neighbors(String sourceNode,double x, double threshold){
        graphAlgorithms.x_weighted_Neighbors(ppi,sourceNode,x,threshold);
    }

    public void x_weighted_neighbors(String sourceNode,double x,double threshold){
        graphAlgorithms.weighted_Neighbors(ppi,sourceNode,x,threshold);
    }

    public Dataset<Row> closestComponent(ArrayList<Object> N){

        Dataset<Row> components=ppi.connectedComponents().run();

        Tuple2<Long, Integer> max=components.javaRDD()
                    .mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
                    .mapToPair(t->{
                        if(N.contains(t._2)) {
                            return new Tuple2<>(Long.parseLong(t._1.toString()),1);
                        }
                        else {
                            return new Tuple2<>(Long.parseLong(t._1.toString()),0);
                        }
                    })
                    .reduceByKey((i1,i2)->{return i1+i2;})
                    .max((t1,t2)->{
                        if(t1._2>=t2._2) {
                            return 1;
                        }
                        return -1;
                    });
        Dataset<Row> maxComponent=components.filter("component="+max._1);

        return maxComponent;
    }

    public void intersectionByComponent(List<String> complex){
        List<String> included=complex.subList(0,20);
        Dataset<Row> components=ppi.connectedComponents().run();
        JavaRDD<Row> complex179=components
                .javaRDD()
                .filter(r->complex.contains(r.get(0)));

        for(Row r:complex179.collect()){
            System.out.println(r.get(0)+" "+r.get(1));
        }


        JavaPairRDD<Long, Integer> intersections=components.javaRDD()
                .mapToPair(r->new Tuple2<>(r.get(1),r.get(0)))
                .mapToPair(t->{
                        if(included.contains(t._2)) {
                            return new Tuple2<>(Long.parseLong(t._1.toString()),1);
                        }
                        else {
                            return new Tuple2<>(Long.parseLong(t._1.toString()), 0);
                        }
                    })
                .reduceByKey((i1,i2)->{return i1+i2;})
                .filter(t->t._2>0);

                intersections.coalesce(1)
                .saveAsTextFile("components intersection");
    }
}

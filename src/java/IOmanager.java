package ppispark.util;

import com.mongodb.spark.MongoSpark;
import jdk.internal.net.http.frame.DataFrame;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import ppiscala.graphUtil;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.HashMap;

public class IOmanager {
    //TSV FILE

    public static GraphFrame importFromTsv(SparkSession spark, String path, String[] cols_names) {
        Dataset<Row> edges = spark.read().
                option("header", "True").
                option("sep", "\t").format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"). //delimiter?
                load(path);

        edges = edges.toDF(cols_names);
        GraphFrame graph = GraphFrame.fromEdges(edges);
        return graph;
    }

    public static void exportToTsv(SparkSession spark, GraphFrame g, String outputName) {
        spark.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        spark.sparkContext().hadoopConfiguration().set("parquet.enable.summary-metadata", "false");

        g.edges().coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "true").option("delimiter", "\t").save(outputName);
    }


    //NEO4J:import functions
    public static GraphFrame importFromNeo4j(SparkSession spark, String url, String user, String password, boolean vertices, String v_prop) {
        if (!vertices) {
            return GraphFrame.fromEdges(graphUtil.edgesFromNeo4j(spark, url, user, password));
        } else {
            return GraphFrame.apply(graphUtil.nodesFromNeo4j(spark, url, user, password, v_prop), graphUtil.edgesFromNeo4j(spark, url, user, password));
        }
    }

    public static GraphFrame importFromNeo4j(SparkSession spark, String url, String user, String password) {
        return importFromNeo4j(spark, url, user, password, false, "");
    }

    public static GraphFrame importFromNeo4j(SparkSession spark, String url, String user, String password, String propRef, String condition, boolean vertices, String v_prop) {
        if (!vertices) {
            return GraphFrame.fromEdges(graphUtil.edgesFromNeo4j(spark, url, user, password, propRef, condition));
        } else {
            return GraphFrame.apply(graphUtil.nodesFromNeo4j(spark, url, user, password, v_prop), graphUtil.edgesFromNeo4j(spark, url, user, password, propRef, condition));
        }
    }

    public static GraphFrame importFromNeo4j(SparkSession spark, String url, String user, String password, String propRef, String condition) {
        return importFromNeo4j(spark, url, user, password, propRef, condition, false, "");
    }

    public static GraphFrame importFromNeo4j(SparkSession spark, String url, String user, String password, String filters, boolean toProp, boolean vertices, String v_prop) {
        if (toProp && !vertices) {
            return GraphFrame.fromEdges(graphUtil.edgesFromNeo4j(spark, url, user, password, filters));
        } else if (!toProp && !vertices) {
            return GraphFrame.fromEdges(graphUtil.filteredEdgesFromNeo4j(spark, url, user, password, filters));
        } else if (toProp && vertices) {
            return GraphFrame.apply(graphUtil.nodesFromNeo4j(spark, url, user, password, v_prop), graphUtil.edgesFromNeo4j(spark, url, user, password, filters));
        } else {
            return GraphFrame.apply(graphUtil.filteredEdgesFromNeo4j(spark, url, user, password, filters), graphUtil.edgesFromNeo4j(spark, url, user, password, filters));
        }
    }
    public static GraphFrame importFromNeo4j(SparkSession spark, String url, String user, String password, String filters, boolean toProp) {
        return importFromNeo4j(spark, url, user, password, filters, toProp, false, "");
    }

    //NEO4j:export functions

    public static void exporttoNeo4j(Dataset<Row> df,String url,String user,String password){
        graphUtil.graphToNeo4J(df,url,user,password);
    }

    public static void updateNodes(Dataset<Row> df,String url,String user,String password,String attr,String ref_col,String properties){
        graphUtil.updateVertices(df,url,user,password,attr,ref_col,properties);
    }
}
package ppiscala

import org.apache.arrow.vector.dictionary.Dictionary

import java.util
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.rand
import org.graphframes.GraphFrame

import scala.collection.JavaConverters

object graphAlgorithms {

  def generate_seeds(g:GraphFrame,n:Int): util.ArrayList[String] ={
    val seeds:util.ArrayList[String]=new util.ArrayList[String]()

    g.vertices.take(n).foreach(t=>seeds.add(t.getString(0)))
    for(i<-0 to seeds.size()-1)(print("\"'"+seeds.get(i)+"'\" "))
    seeds
  }
  def xSubGraph(g: GraphFrame, n: Int, x: Double)={
    val seeds:util.ArrayList[String]=new util.ArrayList[String]()
    g.vertices.take(n).foreach(t=>seeds.add(t.getString(0)))
    for(i<-0 to seeds.size()-1)(print(",'"+seeds.get(i)+"'"))


    val start=System.currentTimeMillis()
    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), if (seeds.contains(attr.getString(0)))  0.0 else Double.PositiveInfinity)
      vertex
    })

    val distanceGraph = initialGraph.pregel(Double.PositiveInfinity,activeDirection=EdgeDirection.Either)(
      (_, attr, newDist) => (attr._1, math.min(attr._2, newDist)),
      triplet => {
        //Distance accumulator
        if (triplet.srcAttr._2 + 1 < triplet.dstAttr._2 && triplet.srcAttr._2 + 1 <= x ) {
          Iterator((triplet.dstId, triplet.srcAttr._2 + 1))
        } else if(triplet.dstAttr._2 + 1 < triplet.srcAttr._2 && triplet.dstAttr._2 + 1 <= x){
          Iterator((triplet.srcId, triplet.dstAttr._2 + 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )

    //distanceGraph.vertices.filter(t=>t._2._2<Double.PositiveInfinity).foreach(println)
    println(distanceGraph.vertices.filter(t=>t._2._2<Double.PositiveInfinity).count())
    distanceGraph.vertices.filter(t=>t._2._2<Double.PositiveInfinity).values.map(t=>(t._2,t._1)).sortByKey().foreach(println)
    System.out.println("xSubGraph: " + (System.currentTimeMillis - start) / 1000.0 / 60)
  }

  def find_connection(g: GraphFrame, sourceNode:String): JavaRDD[Row] = {



    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), if (attr.getString(0) == sourceNode) 1 else 0)
      vertex
    })

    val distanceGraph = initialGraph.pregel(0,activeDirection=EdgeDirection.Either)(
      (_, attr, newDist) => (attr._1, attr._2 + newDist),
      triplet => {
        //Distance accumulator
        if (triplet.srcAttr._2==1 && triplet.dstAttr._2==0 ) {
          Iterator((triplet.dstId, 1))
        } else if(triplet.srcAttr._2==0 && triplet.dstAttr._2==1){
          Iterator((triplet.srcId, 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a+b
    )

   distanceGraph
     .mapTriplets(t=>(t.srcAttr._1,t.dstAttr._1,t.attr.getString(0),t.dstAttr._2))
     .edges
     .filter(t=>t.attr._4==1)
     .map(t=>Row(t.attr._2,t.attr._1,t.attr._3)).toJavaRDD()
  }



//  def x_neighbors_graph(g: GraphFrame, sourceNode:String, x: Double): Graph[(String,Double),Row] = {
//
//    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
//      val vertex = (attr.getString(0), if (attr.getString(0) == sourceNode) 0.0 else Double.PositiveInfinity)
//      vertex
//    })
//
//    val distanceGraph = initialGraph.pregel(Double.PositiveInfinity,activeDirection=EdgeDirection.Either)(
//      (_, attr, newDist) => (attr._1, math.min(attr._2, newDist)),
//      triplet => {
//        //Distance accumulator
//        val combined_score=triplet.attr.getString(2).toDouble/1000
//        if (triplet.srcAttr._2 + 1 < triplet.dstAttr._2 && triplet.srcAttr._2 + 1 <= x) {
//          Iterator((triplet.dstId, triplet.srcAttr._2 + 1))
//        } else if(triplet.dstAttr._2 + 1 < triplet.srcAttr._2 && triplet.dstAttr._2 + 1 <= x){
//          Iterator((triplet.srcId, triplet.dstAttr._2 + 1))
//        } else {
//          Iterator.empty
//        }
//      },
//      (a, b) => math.min(a, b)
//    )
//
//    distanceGraph=distanceGraph.vertices.filter(vertex=>vertex._2._2==1)
//
//    return distanceGraph
//  }
def weighted_Neighbors(g: GraphFrame, sourceNode:String, threshold :Double)= {

  val initialGraph = g.toGraphX.mapVertices((_, attr) => {
    val vertex = (attr.getString(0), if (attr.getString(0) == sourceNode) 1 else -1.0)
    vertex
  })

  //print(initialGraph.edges.take(1)(0).attr.getString(2).toDouble)

  val distanceGraph = initialGraph.pregel(-1.0,activeDirection=EdgeDirection.Either)(
    (_, attr, newDist) => (attr._1,Math.max(newDist,attr._2)),
    triplet => {
      //Distance accumulator
      val combined_score=triplet.attr.getString(2).toDouble
      if (combined_score*triplet.srcAttr._2>threshold&&combined_score*triplet.srcAttr._2!=triplet.dstAttr._2&&triplet.dstAttr._2!=1) {
        Iterator((triplet.dstId, Math.abs(combined_score*triplet.srcAttr._2)))
      } else if(combined_score*triplet.dstAttr._2>threshold&&combined_score*triplet.dstAttr._2!=triplet.srcAttr._2&&triplet.srcAttr._2!=1){
        Iterator((triplet.srcId, Math.abs(combined_score*triplet.dstAttr._2)))
      } else {
        Iterator.empty
      }
    },
    (a, b) => Math.max(a,b)
  )

  distanceGraph.vertices.values.filter(v=>v._2>0).coalesce(1).saveAsTextFile("neighbors")
}

  def weighted_Neighbors(g: GraphFrame, sourceNode:String, x:Double,threshold :Double)= {

    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), if (attr.getString(0) == sourceNode) 0.0 else Double.PositiveInfinity,1.0)
      vertex
    })

    val distanceGraph = initialGraph.pregel((Double.PositiveInfinity,1.0),activeDirection=EdgeDirection.Either)(
      (_, attr, newDist) => if(attr._2<newDist._1) {attr} else{(attr._1,newDist._1,newDist._2)},
      triplet => {
        //Distance accumulator
        val combined_score=triplet.attr.getString(2).toDouble
        if (triplet.srcAttr._2 + 1 < triplet.dstAttr._2 && triplet.srcAttr._2 + 1 <= x && combined_score*triplet.srcAttr._3>threshold) {
          Iterator((triplet.dstId, (triplet.srcAttr._2 + 1,combined_score*triplet.srcAttr._3)))
        } else if(triplet.dstAttr._2 + 1 < triplet.srcAttr._2 && triplet.dstAttr._2 + 1 <= x && combined_score*triplet.dstAttr._3>threshold){
          Iterator((triplet.srcId, (triplet.dstAttr._2 + 1,combined_score*triplet.dstAttr._3)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => if(a._1<b._1) a else b
    )
    distanceGraph.vertices.values.filter(v=>v._2<Double.PositiveInfinity).coalesce(1).saveAsTextFile("neighbors_intact")
  }

  def x_weighted_Neighbors(g: GraphFrame, sourceNode:String, x: Double,threshold :Double)= {

    g.toGraphX.collectNeighbors(EdgeDirection.Either)
    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (attr.getString(0), if (attr.getString(0) == sourceNode) 0.0 else Double.PositiveInfinity)
      vertex
    })

    //print(initialGraph.edges.take(1)(0).attr.getString(2).toDouble)

    val distanceGraph = initialGraph.pregel(Double.PositiveInfinity,activeDirection=EdgeDirection.Either)(
      (_, attr, newDist) => (attr._1,math.min(attr._2, newDist)),
      triplet => {
        //Distance accumulator
        val combined_score=triplet.attr.getString(2).toDouble//string->1000
        if (triplet.srcAttr._2 + 1 < triplet.dstAttr._2 && triplet.srcAttr._2 + 1 <= x && combined_score>threshold) {
          Iterator((triplet.dstId, triplet.srcAttr._2 + 1))
          } else if(triplet.dstAttr._2 + 1 < triplet.srcAttr._2 && triplet.dstAttr._2 + 1 <= x && combined_score>threshold){
            Iterator((triplet.srcId, triplet.dstAttr._2 + 1))
          } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )

    distanceGraph.vertices.values.filter(v=>v._2<Double.PositiveInfinity).coalesce(1).saveAsTextFile("neighbors")
  }

  def xNeighbors(g: GraphFrame,n:Int,x: Double)={
    val seeds:util.ArrayList[String]=new util.ArrayList[String]()
    g.vertices.take(n).foreach(t=>seeds.add(t.getString(0)))

    val start=System.currentTimeMillis()
    for(i<-0 to seeds.size()-1)(print("'"+seeds.get(i)+"' "))

    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (attr.getString(0),Array.fill[Double](seeds.size())(Double.PositiveInfinity))
      val idx = seeds.indexOf(attr.getString(0))

      if (idx > -1)
        vertex._2(idx) = 0

      vertex
    })

    val distance = initialGraph.pregel(Array.fill(seeds.size())(Double.PositiveInfinity),activeDirection=EdgeDirection.Either)(
      (_, vertex, newDist) => {
        val minDist = Array.ofDim[Double](newDist.length)

        for (i <- newDist.indices)
          minDist(i) = math.min(vertex._2(i), newDist(i))

        (vertex._1, minDist)
      },
      triplet => {
        val minDist = Array.ofDim[Double](triplet.srcAttr._2.length)
        var updated_src = false
        var updated_dst=false

        for (i <- triplet.srcAttr._2.indices) {
          if (triplet.srcAttr._2(i) + 1 < triplet.dstAttr._2(i) && triplet.srcAttr._2(i) + 1<=x) {
            minDist(i) = triplet.srcAttr._2(i) + 1
            updated_src = true
          }
          else if(triplet.dstAttr._2(i) + 1 < triplet.srcAttr._2(i) && triplet.dstAttr._2(i) + 1<=x) {
            minDist(i) = triplet.dstAttr._2(i) + 1
            updated_dst = true
          }else{
            minDist(i) = Double.PositiveInfinity
          }
        }

        if (updated_src && updated_dst)
          Iterator((triplet.dstId, minDist),(triplet.srcId, minDist))
        else if(updated_src && !updated_dst)
          Iterator((triplet.dstId, minDist))
        else if(updated_dst && !updated_src)
          Iterator((triplet.srcId, minDist))
        else
          Iterator.empty
      }
      ,
      (dist1, dist2) => {
        val minDist = Array.ofDim[Double](dist1.length)

        for (i <- dist1.indices)
          minDist(i) = math.min(dist1(i), dist2(i))

        minDist
      }
    )


    distance.vertices.values.foreach(t=>{
      for(i<-t._2.indices){
        if(t._2(i)<=x){
          println(t._1+" "+seeds.get(i))
        }
      }
    })
//    println(distance.vertices.values.map(v=>{
//      var neighbor=false
//      var counter=0
//      while(!neighbor && counter<v._2.length){
//        if(v._2(counter)!=Double.PositiveInfinity){
//          neighbor=true
//        }
//        counter+=1
//      }
//      if(neighbor)
//        (v._1,true)
//      else
//        (v._1,false)
//    }).filter(v=>v._2).count())
    //System.out.println("Time(m): " + (System.currentTimeMillis - start) / 1000.0 / 60)

  }

  def xNeighbors_beta(g: GraphFrame,n:Int,x: Double)={
    val seeds:util.ArrayList[String]=new util.ArrayList[String]()
    g.vertices.take(n).foreach(t=>seeds.add(t.getString(0)))

    val start=System.currentTimeMillis()
    for(i<-0 to seeds.size()-1)(print("\"'"+seeds.get(i)+"'\" "))

    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (Array.fill[Double](seeds.size())(Double.PositiveInfinity))
      val idx = seeds.indexOf(attr.getString(0))

      if (idx > -1)
        vertex(idx) = 0

      vertex
    })

    val distance = initialGraph.pregel(Array.fill(seeds.size())(Double.PositiveInfinity),activeDirection=EdgeDirection.Either)(
      (_, vertex, newDist) => {
        val minDist = Array.ofDim[Double](newDist.length)

        for (i <- newDist.indices)
          minDist(i) = math.min(vertex(i), newDist(i))

        (minDist)
      },
      triplet => {
        val minDist = Array.ofDim[Double](triplet.srcAttr.length)
        var updated_src = false
        var updated_dst=false

        for (i <- triplet.srcAttr.indices) {
          if (triplet.srcAttr(i) + 1 < triplet.dstAttr(i) && triplet.srcAttr(i) + 1<=x) {
            minDist(i) = triplet.srcAttr(i) + 1
            updated_src = true
          }
          else if(triplet.dstAttr(i) + 1 < triplet.srcAttr(i) && triplet.dstAttr(i) + 1<=x) {
            minDist(i) = triplet.dstAttr(i) + 1
            updated_dst = true
          }else{
            minDist(i) = Double.PositiveInfinity
          }
        }

        if (updated_src && updated_dst)
          Iterator((triplet.dstId, minDist),(triplet.srcId, minDist))
        else if(updated_src && !updated_dst)
          Iterator((triplet.dstId, minDist))
        else if(updated_dst && !updated_src)
          Iterator((triplet.srcId, minDist))
        else
          Iterator.empty
      }
      ,
      (dist1, dist2) => {
        val minDist = Array.ofDim[Double](dist1.length)

        for (i <- dist1.indices)
          minDist(i) = math.min(dist1(i), dist2(i))

        minDist
      }
    )

    distance.vertices.foreach(t=>{
      print(t._1+" ")
      t._2.foreach(x=>print(x+" "))
      println(" ")
    })
    //    println(distance.vertices.values.map(v=>{
    //      var neighbor=false
    //      var counter=0
    //      while(!neighbor && counter<v._2.length){
    //        if(v._2(counter)!=Double.PositiveInfinity){
    //          neighbor=true
    //        }
    //        counter+=1
    //      }
    //      if(neighbor)
    //        (v._1,true)
    //      else
    //        (v._1,false)
    //    }).filter(v=>v._2).count())
    System.out.println("Time(m): " + (System.currentTimeMillis - start) / 1000.0 / 60)

  }

  def xNeighbors(g: GraphFrame, seeds: util.ArrayList[String], x: Double,threshold:Double)={


    val initialGraph = g.toGraphX.mapVertices((_, attr) => {
      val vertex = (attr.getString(0),Array.fill[Double](seeds.size())(Double.PositiveInfinity))
      val idx = seeds.indexOf(attr.getString(0))

      if (idx > -1)
        vertex._2(idx) = 0

      vertex
    })

    val distance = initialGraph.pregel(Array.fill(seeds.size())(Double.PositiveInfinity),activeDirection=EdgeDirection.Either)(
      (_, vertex, newDist) => {
        val minDist = Array.ofDim[Double](newDist.length)

        for (i <- newDist.indices)
          minDist(i) = math.min(vertex._2(i), newDist(i))

        (vertex._1, minDist)
      },
      triplet => {
        val minDist = Array.ofDim[Double](triplet.srcAttr._2.length)
        var updated_src = false
        var updated_dst=false
        val combined_score=triplet.attr.getDouble(2)/1000

        if(combined_score<threshold)
          for (i <- triplet.srcAttr._2.indices)
            if (triplet.srcAttr._2(i) + 1 < triplet.dstAttr._2(i) && triplet.srcAttr._2(i) + 1<=x) {
              minDist(i) = triplet.srcAttr._2(i) + 1
              updated_src = true
            }
            else if(triplet.dstAttr._2(i) + 1 < triplet.srcAttr._2(i) && triplet.dstAttr._2(i) + 1<=x) {
              minDist(i) = triplet.dstAttr._2(i) + 1
              updated_dst = true
            }else{
              minDist(i) = triplet.dstAttr._2(i)
            }

        if (updated_src && updated_dst)
          Iterator((triplet.dstId, minDist),(triplet.srcId, minDist))
        else if(updated_src && !updated_dst)
          Iterator((triplet.dstId, minDist))
        else if(updated_dst && !updated_src)
          Iterator((triplet.srcId, minDist))
        else
          Iterator.empty
        }
      ,
      (dist1, dist2) => {
        val minDist = Array.ofDim[Double](dist1.length)

        for (i <- dist1.indices)
          minDist(i) = math.min(dist1(i), dist2(i))

        minDist
      }
    )

    distance.vertices.values.foreach(t=>{
      print(t._1+" ")
      t._2.foreach(println)
    })

    println(distance.vertices.values.map(v=>{
      var neighbor=false
      var counter=0
      while(!neighbor && counter<v._2.length){
        if(v._2(counter)!=Double.PositiveInfinity){
          neighbor=true
        }
        counter+=1
      }
      if(neighbor)
        (v._1,true)
      else
        (v._1,false)
    }).filter(v=>v._2).count())

  }


}

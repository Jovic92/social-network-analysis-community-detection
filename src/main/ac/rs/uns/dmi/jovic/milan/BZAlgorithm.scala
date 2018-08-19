package ac.rs.uns.dmi.jovic.milan

import org.apache.spark.graphx.{Graph, _}

import scala.reflect.ClassTag


object BZAlgorithm {


  def doBZAlgorithm[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {

    val t = System.currentTimeMillis();

    val E: Long = 0
    var md: Int = 0 //max degree
    val n: Long = graph.numVertices;
    val vertexRDD: VertexRDD[VD] = graph.vertices;

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    val vertexRDD2: VertexRDD[Int] = graph.outDegrees;

    val maxOutDegree2: (VertexId, Int) = graph.outDegrees.reduce(max)

    md = maxOutDegree2._2;
    var vert = new Array[Int](n.toInt)
    var pos = new Array[Int](n.toInt)
    var deg = new Array[Int](n.toInt)
    var bin = new Array[Int](md + 1) //md+1 because we can have zero degree
    var v: Int = 0;

    val x: Array[(VertexId, Int)] = vertexRDD2.collect()

    for (node <- x) {
      deg(v) = node._2;
      bin(deg(v)) += 1
      v += 1
    }

    var start: Int = 0; //start=1 in original, but no problem
    var d = 0
    while (d <= md) {
      val num = bin(d)
      bin(d) = start
      start += num
      d += 1
    }

    var vv = 0
    while (vv < n) {
      pos(vv) = bin(deg(vv))
      vert(pos(vv)) = vv
      bin(deg(vv)) += 1
      vv += 1
    }

    var d2: Integer = md;

    while (d2 >= 1) {
      bin(d2) = bin(d2 - 1)
      d2 -= 1
    }

    bin(0) = 0

    var i: Int = 0
    val x1: Array[(VertexId, Int)] = vertexRDD2.collect()
    val x2: Array[(VertexId, VD)] = vertexRDD.collect()

    for (cvrc <- x1) {
      var pom: Int = vert(i); //smallest degree vertex
      var v_deg: Int = cvrc._2;

      val direction: EdgeDirection = EdgeDirection.Either
      val neighbours: Seq[Array[(VertexId, VD)]] = graph.collectNeighbors(direction).lookup(cvrc._1)
      val distinctNeighbours : Array[(VertexId, VD)] = neighbours(0).distinct;

      for (nk <- distinctNeighbours) {
        val u: Int = x2.indexOf(nk)
        if (deg(u) > deg(pom)) {
          val du = deg(u)
          val pu = pos(u)
          val pw = bin(du)
          val w: Int = vert(pw)
          if (u != w) {
            pos(u) = pw
            vert(pu) = w
            pos(w) = pu
            vert(pw) = u
          }
          bin(du) += 1
          deg(u) -= 1
        }
      }
      i += 1
    }

    var kmax: Int = -(1)
    var sum: Double = 0
    var cnt = 0
    var y = 0

    for (cvrc <- x1) {
      if (deg(y) > kmax)
        kmax = deg(y)
      sum += deg(y)

      if (deg(y) > 0) {
        cnt += 1
      }
      y += 1
    }

    val tf = System.currentTimeMillis();



    System.out.println("|V|	|E|	dmax	kmax	kavg")
    System.out.println(cnt + "\t" + graph.numEdges + "\t" + md + "\t" + kmax + "\t" + (sum / cnt))
    println(tf-t)
  }

}

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

case class Person(id: Long, name: String)

case class Movie(id: Long, title: String)

case class Acting(actorId: Long, movieId: Long)

case class Direction(directorId: Long, movieId: Long)


object GraphXExample extends App {
  type Person = String
  type Movie = String


  def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
    def minThatExists(k: VertexId): Int = {
      math.min(
        m1.getOrElse(k, Int.MaxValue),
        m2.getOrElse(k, Int.MaxValue))
    }

    (m1.keySet ++ m2.keySet).map {
      k => (k, minThatExists(k))
    }.toMap
  }

  def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int])
  : Map[VertexId, Int] = {
    mergeMaps(state, msg)
  }

  def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId)
  : Iterator[(VertexId, Map[VertexId, Int])] = {
    val aplus = a.map { case (v, d) => v -> (d + 1) }
    if (b != mergeMaps(aplus, b)) {
      Iterator((bid, aplus))
    } else {
      Iterator.empty
    }
  }

  def iterate(e: EdgeTriplet[Map[VertexId, Int], _]):
  Iterator[(VertexId, Map[VertexId, Int])] = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
      checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
  }

  def readFile(filename: String) = {
    val src = Source.fromFile(filename)
    val lines = src.getLines().toList.drop(1).map(_.split(","))
    src.close()
    lines
  }

  def readJson(filename: String) = {
    val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val src = Source.fromFile(filename)(decoder)
    val lines = src.getLines().toList
    val data = lines.map(ujson.read(_))
    data
  }

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val movies = readJson("movies.json")
    .map(movieJson => {
      val movie = Movie(
        movieJson("id").num.toLong,
        movieJson("title").str
      )
      movie.id -> movie.title
    }).toMap

  val persons = readJson("persons.json")
    .map(personJson => {
      val person = Person(
        personJson("id").num.toLong,
        personJson("name").str
      )
      person.id -> person.name
    }
    )

  val acts_in = readJson("acts_in.json")
    .map(actingJson => {
      val acting = Acting(
        actingJson("from_id").num.toLong,
        actingJson("to_id").num.toLong
      )
      acting
    })
    .groupBy(acting => acting.movieId).mapValues(acting => acting.map(_.actorId))

  val directed = readJson("directed.json")
    .map(directionJson => {
      val direction = Direction(
        directionJson("from_id").num.toLong,
        directionJson("to_id").num.toLong
      )
      direction.movieId -> direction.directorId
    })
    .toMap

  def getDirector(movieId: Long) = {
    try {
      List(directed(movieId))
    } catch {
      case _ => List.empty
    }
  }

  def getActors(movieId: Long) = {
    try {
      acts_in(movieId)
    } catch {
      case _ => List.empty
    }
  }


  val edges = movies.flatMap { case (movieId, movieTitle) =>
    val personIds = getDirector(movieId) ::: getActors(movieId)
    val pairs = personIds.toSet.subsets(2).toList
    pairs.flatMap(pair => {
      val List(p1, p2) = pair.toList
      if (p1 != p2) {
        List(Edge(p1, p2, movieTitle), Edge(p2, p1, movieTitle))
      }
      else List()
    })
  }.toSeq


  val conf = new SparkConf().setAppName("GraphX Example").setMaster("local")
  val sc = new SparkContext(conf)

  val personNodes: RDD[(VertexId, Person)] = sc.parallelize(persons)
  val personEdges: RDD[Edge[String]] = sc.parallelize(edges)

  val graph = Graph(personNodes, personEdges).groupEdges((m1, m2) => m1 + "," + m2).cache()

  val cc = graph.connectedComponents().cache()
  val orderedComponents = cc.vertices
    .groupBy { case (vid, cc) => cc }
    .mapValues(list => list.count(_ => true))
    .filter(_._2 != 1)
    .sortBy(_._2)

  println("------------------------- Zadanie 1 -------------------------\nNajmniejsze komponenty składowe")
  val smallestComponents = orderedComponents.take(5)
  smallestComponents.foreach(println)

  println(s"Liczba spójnych składowych: ${orderedComponents.count()} - większa od 0, więc graf nie jest spójny.")

  val joined = graph.outerJoinVertices(cc.vertices) {
    (vid, vd, cc) => (vd, cc)
  }

  println("------------------------- Zadanie 2 -------------------------")
  val alienated = joined.subgraph(vpred = {
    (vid, vdcc) => vdcc._2.contains(smallestComponents.take(1).head._1)
  }).mapVertices((vid, vdcc) => vdcc._1).vertices

  println("Przykład wyalienowanych osób")
  alienated.foreach(println)

  println(s"------------------------- Zadanie 3 -------------------------")
  println("dla Bacona")
  val kevinBaconId = graph.vertices.filter(_._2 == "Kevin Bacon").collect.head._1
  val baconComponentId = cc.vertices.filter(_._1 == kevinBaconId).collect().head._2

  val baconGraph = joined.subgraph(vpred = {
    (vid, vdcc) => vdcc._2.contains(baconComponentId)
  }).mapVertices((vid, vdcc) => vdcc._1).cache()

  val degreesBaconVertex = baconGraph.degrees.cache()
  val degreesBaconVertexStats = degreesBaconVertex.map(_._2).stats()
  println(s"Bacon vertex stats: ${degreesBaconVertexStats}")
  val maxActor = baconGraph.vertices.lookup(degreesBaconVertex.filter(_._2 == degreesBaconVertexStats.max).take(1).head._1).head
  println(s"Actor with most relations: ${maxActor}")
  println(s"Bacon's relations: ${degreesBaconVertex.lookup(kevinBaconId).head}")

  // trzeba ogarnąć Zeppelin
  //  val degreesAmazonDF = degreesAmazonVertex.toDF
  //  degreesAmazonDF.createOrReplaceTempView("amazon")
  //  %sql
  //    select `_2` as degree, count(*) as count
  //  from `amazon`
  //    group by `_2`
  //  order by count

  println(s"------------------------- Zadanie 4 -------------------------")
  val degreesVertex = graph.degrees
  val maxTriangles = degreesVertex.mapValues(k => k * (k - 1) / 2.0)
  val triCountGraph = graph.triangleCount()
  val clusterCoef = triCountGraph.vertices.
    innerJoin(maxTriangles) {
      (vertexId, triCount, maxTris) => if (maxTris == 0) 0 else triCount / maxTris
    }
    .map(_._2).sum() / graph.vertices.count()
  println(s"Współczynnik klastrowania:  ${clusterCoef}")



  println(s"------------------------- Zadanie 5 -------------------------")
  val start = Map[VertexId, Int]()
  val preparedGraph = baconGraph.mapVertices((id, v) => Map(id -> 0))
  val res = preparedGraph.ops.pregel(start)(update, iterate, mergeMaps)
  val paths = res.vertices.flatMap {
    case (id, m) =>
      m.map { case (k, v) => if (id < k) {
        (id, k, v)
      } else {
        (k, id, v)
      }
      }
  }.distinct().cache()
  val baconStats = paths.map(_._3).filter(_ > 0).stats()

  println(s"Statystyki dla Bacona: ${baconStats}")
}
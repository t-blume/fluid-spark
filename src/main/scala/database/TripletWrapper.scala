package database

import org.apache.spark.graphx.EdgeTriplet

class TripletWrapper(triplets: java.util.HashSet[EdgeTriplet[Set[(String, String)], (String, String, String, String)]]) extends Serializable {

  def this(t: EdgeTriplet[Set[(String, String)], (String, String, String, String)]) {
    this(new java.util.HashSet[EdgeTriplet[Set[(String, String)], (String, String, String, String)]]())
    triplets.add(t)
  }

  def merge(other: TripletWrapper): TripletWrapper = {
    triplets.addAll(other.triplets)
    this
  }

  def triplets():  java.util.HashSet[EdgeTriplet[Set[(String, String)], (String, String, String, String)]] = {
    triplets
  }

}

package com.containers.infrastrcture

import com.twitter.scalding.typed.TypedPipe
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * User: netab
  * Date: 5/2/16
  */
object Containers {


  trait HasContainerFunctions[C[_]] {

    def filter[T](container : C[T])(f: T => Boolean) : C[T]

    def map[T, U: ClassTag](container : C[T])(f: T => U) : C[U]

    def flatMap[T, U: ClassTag](container : C[T])(f: T => TraversableOnce[U]) : C[U]

    def flatten[T : ClassTag](container : C[TraversableOnce[T]]) : C[T] = flatMap(container)(identity)

    def distinct[T](container : C[T])(implicit ord: Ordering[_ >: T]) : C[T]

    def mapValues[T: ClassTag, K: ClassTag, U](container: C[(K, T)])(f: T => U): C[(K, U)]

    def leftOuterJoin[T: ClassTag, K: ClassTag, U](container: C[(K, T)])
                                                  (other: C[(K, U)])
                                                  (implicit ord : Ordering[K]): C[(K, (T, Option[U]))]

    def innerJoin[T: ClassTag, K: ClassTag, U](container: C[(K, T)])
                                              (other: C[(K, U)])
                                              (implicit ord : Ordering[K]): C[(K, (T, U))]

    def aggregateByKey[T: ClassTag, K: ClassTag, U: ClassTag](container : C[(K, T)])
                                                             (zeroValue : U)
                                                             (seqOp : (U, T) => U, combOp: (U, U) => U)
                                                             (implicit ord : Ordering[K]) : C[(K, U)]

    def persist[T](container : C[T]) : C[T]

    def filterByFewValues[T, U](container : C[T])(values : C[U])(f : T => U)(implicit ord : Ordering[U]) : C[T]

    def values[T: ClassTag, K: ClassTag](container: C[(K, T)]) : C[T]

    def listByKey[T: ClassTag, K: ClassTag](container : C[(K, T)])(implicit ord : Ordering[K]) : C[(K, List[T])] =
      aggregateByKey(container)(List.empty[T])((lst, item) => lst :+ item, (lst, lst2) => lst ++ lst2)

    // Each value will appear once in the output
    def countEachValue[T: ClassTag](container : C[T])(implicit ord : Ordering[T]) : C[(T, Long)] =
      aggregateByKey(map(container)((_, 1L)))(0L)(_ + _, _ + _)
  }


  implicit class HasContainerFunctionsOperators[T: ClassTag, C[_]](container : C[T])
                                                                  (implicit hcf : HasContainerFunctions[C]) {

    def filter(f: T => Boolean) : C[T] = hcf.filter(container)(f)

    def map[U: ClassTag](f: T => U) : C[U] = hcf.map(container)(f)

    def flatMap[U: ClassTag](f : T => TraversableOnce[U]) : C[U] = hcf.flatMap(container)(f)

    def distinct(implicit ord: Ordering[_ >: T]) : C[T] = hcf.distinct(container)(ord)

    def persist : C[T] = hcf.persist(container)

    def filterByFewValues[U](values : C[U])(f : T => U)(implicit ord : Ordering[U]) : C[T] = {
      hcf.filterByFewValues(container)(values)(f)
    }

    def flatten[U : ClassTag](implicit conv : T => TraversableOnce[U]) : C[U] = hcf.flatten(container map conv)

    def countEachValue(implicit ord : Ordering[T]) : C[(T, Long)] = hcf.countEachValue(container)
  }


  implicit class HasPairContainerFunctionsOperators[K: ClassTag, V: ClassTag, C[_]](container : C[(K, V)])
                                                                                   (implicit hcf : HasContainerFunctions[C]) {

    def mapValues[U](f: V => U): C[(K, U)] = hcf.mapValues(container)(f)

    def leftOuterJoin[U](other: C[(K, U)])(implicit ord : Ordering[K]): C[(K, (V, Option[U]))] = {
      hcf.leftOuterJoin(container)(other)
    }

    def innerJoin[U](other: C[(K, U)])(implicit ord : Ordering[K]): C[(K, (V, U))] = hcf.innerJoin(container)(other)

    def aggregateByKey[U: ClassTag](zeroValue : U)
                                   (seqOp : (U, V) => U, combOp: (U, U) => U)
                                   (implicit ord : Ordering[K]) : C[(K, U)] = {
      hcf.aggregateByKey(container)(zeroValue)(seqOp, combOp)
    }

    def listByKey(implicit ord : Ordering[K]) : C[(K, List[V])] = hcf.listByKey(container)

    def values : C[V] = hcf.values(container)
  }


  object HasContainerFunctions {

    implicit object RDDContainer extends HasContainerFunctions[RDD] {

      def filter[T](rdd : RDD[T])(f: T => Boolean) : RDD[T] = rdd.filter(f)

      def map[T, U: ClassTag](rdd : RDD[T])(f: T => U) : RDD[U] = rdd.map(f)

      def flatMap[T, U: ClassTag](rdd : RDD[T])(f: T => TraversableOnce[U]) : RDD[U] = rdd.flatMap(f)

      def distinct[T](rdd : RDD[T])(implicit ord: Ordering[_ >: T]) : RDD[T] = rdd.distinct()

      def mapValues[T: ClassTag, K: ClassTag, U](rdd: RDD[(K, T)])(f: T => U): RDD[(K, U)] = {
        new PairRDDFunctions(rdd).mapValues(f)
      }

      def leftOuterJoin[T: ClassTag, K: ClassTag, U](rdd: RDD[(K, T)])
                                                    (other: RDD[(K, U)])
                                                    (implicit ord : Ordering[K]): RDD[(K, (T, Option[U]))] = {
        new PairRDDFunctions(rdd).leftOuterJoin(other)
      }

      def innerJoin[T: ClassTag, K: ClassTag, U](rdd: RDD[(K, T)])(other: RDD[(K, U)])
                                                (implicit ord : Ordering[K]): RDD[(K, (T, U))] = {
        new PairRDDFunctions(rdd).join(other)
      }

      def aggregateByKey[T: ClassTag, K: ClassTag, U: ClassTag](rdd : RDD[(K, T)])
                                                               (zeroValue : U)
                                                               (seqOp : (U, T) => U, combOp: (U, U) => U)
                                                               (implicit ord : Ordering[K]) : RDD[(K, U)] = {
        new PairRDDFunctions(rdd).aggregateByKey(zeroValue)(seqOp, combOp)
      }

      def persist[T](rdd : RDD[T]) : RDD[T] = rdd.persist()

      def filterByFewValues[T, U](rdd : RDD[T])(values : RDD[U])(f : T => U)(implicit ord : Ordering[U]) : RDD[T] = {
        val otherSet = values.distinct.collect.toSet
        rdd.filter{v => otherSet contains f(v)}
      }

      def values[T: ClassTag, K: ClassTag](rdd : RDD[(K, T)]) : RDD[T] = rdd.map(_._2)
    }


    implicit object TypedPipeContainer extends HasContainerFunctions[TypedPipe] {

      def filter[T](pipe : TypedPipe[T])(f : T => Boolean) : TypedPipe[T] = pipe.filter(f)

      def map[T, U: ClassTag](pipe : TypedPipe[T])(f : T => U) : TypedPipe[U] = pipe.map(f)

      def flatMap[T, U: ClassTag](pipe : TypedPipe[T])(f: T => TraversableOnce[U]) : TypedPipe[U] = pipe.flatMap(f)

      def distinct[T](pipe : TypedPipe[T])(implicit ord: Ordering[_ >: T]) : TypedPipe[T] = pipe.distinct(ord)

      def mapValues[T: ClassTag, K: ClassTag, U](pipe: TypedPipe[(K, T)])(f: T => U): TypedPipe[(K, U)] = {
        pipe.mapValues(f)
      }

      def leftOuterJoin[T: ClassTag, K: ClassTag, U](pipe: TypedPipe[(K, T)])(other: TypedPipe[(K, U)])
                                                    (implicit ord : Ordering[K]): TypedPipe[(K, (T, Option[U]))] = {
        pipe.group.leftJoin(other.group).toTypedPipe
      }

      def innerJoin[T: ClassTag, K: ClassTag, U](pipe: TypedPipe[(K, T)])
                                                (other: TypedPipe[(K, U)])
                                                (implicit ord : Ordering[K]): TypedPipe[(K, (T, U))] = {
        pipe.group.join(other.group).toTypedPipe
      }

      def aggregateByKey[T: ClassTag, K: ClassTag, U: ClassTag](pipe : TypedPipe[(K, T)])
                                                               (zeroValue : U)
                                                               (seqOp : (U, T) => U, combOp: (U, U) => U)
                                                               (implicit ord : Ordering[K]) : TypedPipe[(K, U)] = {
        pipe.group.mapValues(seqOp(zeroValue, _)).reduce(combOp).toTypedPipe
      }

      def persist[T](pipe : TypedPipe[T]) : TypedPipe[T] = pipe.forceToDisk

      def filterByFewValues[T, U](pipe : TypedPipe[T])(values : TypedPipe[U])(f : T => U)
                                 (implicit ord : Ordering[U]) : TypedPipe[T] = {
        val valuesPipe = values.distinct(ord).map{e => (e, true)}.group
        pipe.map{e => (f(e), e)}.hashJoin(valuesPipe).values.map{_._1}
      }

      def values[T: ClassTag, K: ClassTag](pipe : TypedPipe[(K, T)]) : TypedPipe[T] = pipe.values
    }


    implicit object ListContainer extends HasContainerFunctions[List] {

      def filter[T](container : List[T])(f: T => Boolean) : List[T] = container.filter(f)

      def map[T, U: ClassTag](container : List[T])(f: T => U) : List[U] = container.map(f)

      def flatMap[T, U: ClassTag](container : List[T])(f: T => TraversableOnce[U]) : List[U] = container.flatMap(f)

      def distinct[T](container : List[T])(implicit ord: Ordering[_ >: T]) : List[T] = container.distinct

      def mapValues[T: ClassTag, K: ClassTag, U](container: List[(K, T)])(f: T => U): List[(K, U)] = {
        container.map{case (k, t) => (k, f(t))}
      }

      def leftOuterJoin[T: ClassTag, K: ClassTag, U](container: List[(K, T)])
                                                    (other: List[(K, U)])
                                                    (implicit ord : Ordering[K]): List[(K, (T, Option[U]))] = {
        val rightMap : Map[K, List[U]] = other.groupBy(_._1).mapValues{lst => lst map {_._2}}
        container flatMap {case (k, v) =>
          rightMap.get(k).map {lst => lst map {v2 => (k, (v, Some(v2)))}}.getOrElse(List((k, (v, None))))}
      }

      def innerJoin[T: ClassTag, K: ClassTag, U](container: List[(K, T)])
                                                (other: List[(K, U)])(implicit ord : Ordering[K]): List[(K, (T, U))] = {
        val rightMap : Map[K, List[U]] = other.groupBy(_._1).mapValues{lst => lst map {_._2}}
        container flatMap {case (k, v) => rightMap.get(k).map {lst => lst map {v2 => (k, (v, v2))}}.getOrElse(List())}
      }

      def aggregateByKey[T: ClassTag, K: ClassTag, U: ClassTag](container : List[(K, T)])
                                                               (zeroValue : U)
                                                               (seqOp : (U, T) => U, combOp: (U, U) => U)
                                                               (implicit ord : Ordering[K]) : List[(K, U)] = {
        container.groupBy{_._1}.mapValues{lst => {lst map {_._2}}.foldLeft(zeroValue)(seqOp)}.toList
      }

      def persist[T](container : List[T]) : List[T] = container

      def filterByFewValues[T, U](container : List[T])(values : List[U])
                                 (f : T => U)(implicit ord : Ordering[U]) : List[T] = {
        val valuesSet = values.toSet
        container.filter{ v => valuesSet contains f(v)}
      }

      def values[T: ClassTag, K: ClassTag](container : List[(K, T)]) : List[T] = container map {_._2}

    }
  }

}
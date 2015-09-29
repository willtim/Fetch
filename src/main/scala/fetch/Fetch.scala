/**
 * Fetch library.
 * Based on ideas in Haxl.
 *
 * Created by willtim on 12/09/15.
 */
package fetch

import java.util.concurrent.ConcurrentHashMap

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** a DataSource both indicates which calls can be batched together and implements the fetch */
trait DataSource[R, A] {
  def fetch(reqs: Seq[R]): Future[Seq[A]]
}

/** The Fetch computation. Follows the API of scala.concurrent.Future. */
sealed case class Fetch[A](result: Cache => Result[A]) {

  @inline def map[B](f: A => B): Fetch[B] = Fetch { cache =>
    result(cache) match {
      case Done(a)             => Done(f(a))
      case Blocked(reqs, cont) => Blocked(reqs, cont map f)
      case Throw(ex)           => Throw(ex)
    }
  }

  @inline def flatMap[B](f: A => Fetch[B]): Fetch[B] = Fetch { cache =>
    result(cache) match {
      case Done(a)             => f(a).result(cache)
      case Blocked(reqs, cont) => Blocked(reqs, cont flatMap f)
      case Throw(ex)           => Throw(ex)
    }
  }

  // applicative in disguise
  @inline def zip[B](that: Fetch[B]): Fetch[(A, B)] = Fetch { cache =>
    (this.result(cache), that.result(cache)) match {
      case (Done(a), Done(b)) => Done((a, b))
      case (Done(a), Blocked(br, f)) => Blocked(br, f map { x => (a, x) })
      case (Blocked(br, f), Done(b)) => Blocked(br, f map { x => (x, b) })
      case (Blocked(br1, f1), Blocked(br2, f2)) => Blocked(br1 ++ br2, f1 zip f2)
      case (Throw(ex), _) => Throw(ex)
      case (_, Throw(ex)) => Throw(ex)
    }
  }

  //def handle(f: Throwable => A): Async[A]         = ???
  //def rescue(f: Throwable => Async[A]): Async[A]  = ???

}

object Fetch {

  /** lift a concrete value */
  def done[A](a: A): Fetch[A] = Fetch { _ => Done(a) }

  /** Sequence the results (not the effects) */
  def sequence[A](fs: Seq[Fetch[A]]): Fetch[Seq[A]] =
    fs.foldLeft(done(Seq[A]())) {
      (fs: Fetch[Seq[A]], f: Fetch[A]) =>
        (fs zip f) flatMap { case (s, a) => done(s :+ a) }
    }

  /** maps a function over the sequence and collects the results */
  def traverse[A, B](fs: Seq[A])(fn: A => Fetch[B]): Fetch[Seq[B]] = sequence { fs map fn }

  /** run the fetch computation */
  def run[A](f: Fetch[A])(implicit ec: ExecutionContext): Future[A] = {
    val cache = new Cache()
    f.result(cache) match {
      case Done(a) => Future.successful(a)
      case Throw(a) => Future.failed(a)
      case Blocked(br, cont) => fetch[A](br, cont)
    }
  }

  private def fetch[A](brs: Seq[BlockedRequest], cont: Fetch[A])(implicit ec: ExecutionContext): Future[A] = {
    val groups = brs.toList groupBy { case DataRequest(_, ds, _) => ds }
    Future.traverse(groups) {
      case (ds, group) =>
        val (requests, promises) = group unzip { case DataRequest(req, _, p) => (req, p) }
        ds.fetch(requests) onComplete {
          case Success(results) =>
            (promises zip results) map {
              case (promise, result) => promise complete Success(result)
            }
          case Failure(ex) => promises map {
            _ complete Failure(ex)
          }
        }
        Future.sequence( promises map { _.future } )
    } flatMap { _ => run(cont) }
  }

  /** create an fetch fetch computation */
  def async[R, A](req: R, ds: DataSource[R, A]): Fetch[A] = Fetch { cache =>
      cache.lookup[A](req) match {
        case None =>
          val p = Promise[A]()
          cache.update(req, p)
          val br = DataRequest[R, A](req, ds, p)
          Blocked(Seq(br), Fetch { _ => cont(p) })
        case Some(p) =>
          if (p.isCompleted) cont(p)
          else Blocked(Seq(), Fetch { _ => cont(p) })
      }
    }

  private def cont[A](p: Promise[A]): Result[A] =
    p.future.value match {
      case Some(Success(value)) => Done(value)
      case Some(Failure(ex)) => Throw(ex)
      case None => throw new AssertionError()
    }

}

sealed trait Result[A]
case class Done[A](a: A) extends Result[A]
case class Blocked[A](reqs: Seq[BlockedRequest], cont: Fetch[A]) extends Result[A]
case class Throw[A](t: Throwable) extends Result[A]

// note the encoding of existential types in DataRequest
trait BlockedRequest
case class DataRequest[R, A](request: R, dataSource: DataSource[R, A], p: Promise[A]) extends BlockedRequest

// correctness of cast determined by correctness of map implementation
class Cache {
  val map: concurrent.Map[Any, Promise[_]] = new ConcurrentHashMap().asScala
  def lookup[A](key: Any): Option[Promise[A]] = map.get(key).map(_.asInstanceOf[Promise[A]])
  def update[A](key: Any, value: Promise[A]) = map.putIfAbsent(key, value)
}

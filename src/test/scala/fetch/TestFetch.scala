package fetch

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Failure

/**
 * Created by tim on 14/09/15.
 */
object TestFetch {

  type PostId = Long
  type PostInfo = String
  type PostContent = String


  // Request types
  sealed trait Request[T]
  case class FetchPosts() extends Request[Seq[PostId]]
  case class FetchPostInfo(postId: PostId) extends Request[PostInfo]
  case class FetchPostContent(postId: PostId) extends Request[PostContent]
  case class FetchPostViews(postId: PostId) extends Request[Int]

  case object PostsSource extends DataSource[FetchPosts, Seq[PostId]] {
    def fetch(reqs: Seq[FetchPosts]) = {
      println("got request for: " ++ reqs.toString)
      Future.successful {
        Seq(Seq(1L,2L,3L))
      }
    }
  }
  case object PostInfoSource extends DataSource[FetchPostInfo, PostInfo] {
    def fetch(reqs: Seq[FetchPostInfo]) = {
      println("got request for: " ++ reqs.toString)
      Future.successful {
        reqs map {
          case FetchPostInfo(l) => "Info" + l.toString
        }
      }
    }
  }
  case object PostContentSource extends DataSource[FetchPostContent, PostContent] {
    def fetch(reqs: Seq[FetchPostContent]) = {
      println("got request for: " ++ reqs.toString)
      Future.successful {
        reqs map {
          case FetchPostContent(l) => "Content" + l.toString
        }
      }
    }
  }
  case object PostViewSource extends DataSource[FetchPostViews, PostContent] {
    def fetch(reqs: Seq[FetchPostViews]) = {
      println("got request for: " ++ reqs.toString)
      Future.successful {
        reqs map {
          case FetchPostViews(l) => "View" + l.toString
        }
      }
    }
  }

  // Services
  val getPostIds = Fetch.async(FetchPosts(), PostsSource)
  def getPostInfo(postId: PostId) = Fetch.async(FetchPostInfo(postId), PostInfoSource)
  def getPostContent(postId: PostId) = Fetch.async(FetchPostContent(postId), PostContentSource)
  def getPostViews(postId: PostId) = Fetch.async(FetchPostViews(postId), PostViewSource)

  def main(args: Array[String]) {
    val popularPosts: Fetch[Seq[PostContent]] = for {
      postIds <- getPostIds
      views <- Fetch.traverse(postIds)(getPostViews)
      ordered = (postIds zip views).sortBy(_._2).map(_._1).take(2)
      content <- Fetch.traverse(ordered)(getPostContent)
    } yield content
    Fetch.run(popularPosts) onComplete {
      case Failure(e) => println(e.getCause)
      case res => println(res)
    }
    Thread.sleep(1000)
  }
  // Fetch.traverse(tweetIds) { tweetId =>
  //   for {
  //     tweet <- getTweet(tweetId)
  //     user  <- geUser(tweet.userId)
  //   } yield (tweet, user)
  // }

  // def get(tweetId: Long): Stitch[Tweet] =
  //   Stitch.call(tweetId, GetTweetBatchGroup)
}

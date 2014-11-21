package nodescala

import scala.language.postfixOps
import scala.util.{ Try, Success, Failure }
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{ async, await }
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be created") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be created") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("All with failure ") {
    val p = for { i <- { 1 to 10 }.toList } yield {
      Future {
        if (i == 5)
          throw new Exception("i is 5")
        else i
      }
    }
    val q = Future.all[Int](p)

    try {
      Await.result(q, 1 nanos)
      assert(false)
    } catch {
      case t: Exception =>
    }
  }

  test("All test with success ") {
    val p = for { i <- { 1 to 10 }.toList } yield Future { i }
    val q = Future.all[Int](p)

    val res = Await.result(q, 5 seconds)
    assert(res.foldLeft(0)((i, l) => i + l) == 55)

  }

  test("Any with failure ") {
    val p = for { i <- { 1 to 10 }.toList } yield {
      Future {
        if (i == 5)
          throw new Exception("i is 5")
        else { Thread.sleep(5); i }
      }
    }
    val q = Future.any[Int](p)

    try {
      Await.result(q, 1 nanos)
      assert(false)
    } catch {
      case t: Exception =>
    }
  }

  test("Any with success ") {
    val p = for { i <- { 1 to 10 }.toList } yield Future {
      if (i == 5)
        i
      else blocking {
        Thread.sleep(500)
        i
      }
    }
    val q = Future.any[Int](p)
    val res = Await.result(q, 1 seconds)
    assert(res === 5)
  }

  test("Now") {
    val p = Future {
      10
    }
    Await.result(p, 5 nanos)
    assert(p.now === 10)
  }

  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Listener should serve the next request as a future") {
    val dummy = new DummyListener(8191, "/test")
    val subscription = dummy.start()

    def test(req: Request) {
      val f = dummy.nextRequest()
      dummy.emit(req)
      val (reqReturned, xchg) = Await.result(f, 1 second)

      assert(reqReturned == req)
    }

    test(immutable.Map("StrangeHeader" -> List("StrangeValue1")))
    test(immutable.Map("StrangeHeader" -> List("StrangeValue2")))

    subscription.unsubscribe()
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}





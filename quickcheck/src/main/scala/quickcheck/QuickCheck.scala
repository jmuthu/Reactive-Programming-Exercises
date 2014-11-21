package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min of two inserts") = forAll { (a: Int, b: Int) =>
    (a < b) ==> {
      val h = insert(b, insert(a, empty))
      findMin(h) == a
    }
  }

  property("insert and delete") = forAll { (a: Int, b: Int) =>
    (a != b) ==> {
      val in = insert(b, insert(a, empty))
      val del = deleteMin(deleteMin(in))
      isEmpty(del) == true
    }
  }

  property("sort with min function with list") = forAll { l: List[Int] =>
    def getSortedList(heap: H): List[Int] = {
      if (isEmpty(heap)) List()
      else findMin(heap) :: getSortedList(deleteMin(heap))
    }
    val h = l.foldLeft(empty)((h, x) => insert(x, h))
    l.sorted == getSortedList(h)
  }

  property("sort and min with heap") = forAll { h: H =>
    def getSortedList(heap: H): List[Int] = {
      if (isEmpty(heap)) List()
      else findMin(heap) :: getSortedList(deleteMin(heap))
    }
    val l = getSortedList(h)
   // println(l)
    l.sorted == l
  }

  property("meld and min with list") = forAll { (l1: List[Int], l2: List[Int]) =>
    (l1 != Nil || l2 != Nil) ==> {
      val h1: H = l1.foldLeft(empty)((h, x) => insert(x, h))
      val h2: H = l2.foldLeft(empty)((h, x) => insert(x, h))

      val min = if (l1 == Nil) l2.min
      else if (l2 == Nil) l1.min
      else if (l1.min < l2.min) l1.min
      else l2.min

      val m = meld(h1, h2)
      findMin(m) == min
    }
  }

  property("meld and min with heap") = forAll { (h1: H, h2: H) =>
    (isEmpty(h1) == false || isEmpty(h2) == false) ==> {
      val min = if (isEmpty(h1)) findMin(h2)
      else if (isEmpty(h2)) findMin(h1)
      else if (findMin(h1) < findMin(h2)) findMin(h1)
      else findMin(h2)
      
      val m = meld(h1, h2)
      findMin(m) == min
    }
  }

  lazy val genHeap: Gen[H] = {
    for {
      l <- Arbitrary.arbitrary[List[A]]
    } yield l.foldLeft(empty)((h, x) => insert(x, h))
  }

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}

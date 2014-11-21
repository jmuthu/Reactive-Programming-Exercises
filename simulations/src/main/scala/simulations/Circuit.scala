package simulations

import common._

class Wire {
  private var sigVal = false
  private var actions: List[Simulator#Action] = List()

  def getSignal: Boolean = sigVal

  def setSignal(s: Boolean) {
    if (s != sigVal) {
      sigVal = s
      actions.foreach(action => action())
    }
  }

  def addAction(a: Simulator#Action) {
    actions = a :: actions
    a()
  }
}

abstract class CircuitSimulator extends Simulator {

  val InverterDelay: Int
  val AndGateDelay: Int
  val OrGateDelay: Int

  def probe(name: String, wire: Wire) {
    wire addAction {
      () =>
        afterDelay(0) {
          println(
            "  " + currentTime + ": " + name + " -> " + wire.getSignal)
        }
    }
  }

  def inverter(input: Wire, output: Wire) {
    def invertAction() {
      val inputSig = input.getSignal
      afterDelay(InverterDelay) { output.setSignal(!inputSig) }
    }
    input addAction invertAction
  }

  def andGate(a1: Wire, a2: Wire, output: Wire) {
    def andAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(AndGateDelay) { output.setSignal(a1Sig & a2Sig) }
    }
    a1 addAction andAction
    a2 addAction andAction
  }

  //
  // to complete with orGates and demux...
  //

  def orGate(a1: Wire, a2: Wire, output: Wire) {
    def andAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(AndGateDelay) { output.setSignal(a1Sig | a2Sig) }
    }
    a1 addAction andAction
    a2 addAction andAction
  }

  def orGate2(a1: Wire, a2: Wire, output: Wire) {
    val notA1, notA2, notAnd = new Wire
    inverter(a1, notA1)
    inverter(a2, notA2)
    andGate(notA1, notA2, notAnd)
    inverter(notAnd, output)
  }

  def demux0(in: Wire, c: List[Wire], out: List[Wire]): Unit = {
    def demux2(in: Wire, c: List[Wire]): List[Wire] = c match {
      case Nil => {
        List(in)
      }
      case head :: tail => {
        val out2 = demux2(in, tail)
        val prefix = for (x <- out2) yield {
          val w = new Wire
          andGate(head, x, w)
          w
        }
        val notHead = new Wire
        inverter(head, notHead)
        val suffix = for (x <- out2) yield {
          val w = new Wire
          andGate(notHead, x, w)
          w
        }
        prefix ++ suffix
      }
    }
    if (c == Nil) {
      out(0).setSignal(in.getSignal)
    } else {
      val output = demux2(in, c.tail)
      for (i <- 0 to output.length - 1) {
        andGate(c.head, output(i), out(i))
      }
      val notHead = new Wire
      inverter(c.head, notHead)
      for (i <- 0 to output.length - 1) {
        andGate(notHead, output(i), out(output.length + i))
      }
    }

  }

  def demux(in: Wire, c: List[Wire], out: List[Wire]): Unit = {
    def demux2(subset: List[Wire]): List[Wire] = subset match {
      case Nil => {
        if (c == Nil) {
          out(0).setSignal(in.getSignal);
          List()
        } else {
          List(in)
        }
      }
      case head :: tail => {
        val output = demux2(tail)
        val notHead = new Wire
        inverter(head, notHead)

        if (subset.length == c.length) {
          for (i <- 0 to output.length - 1) {
            andGate(c.head, output(i), out(i))
            andGate(notHead, output(i), out(output.length + i))
          }
          Nil
        } else {
          val prefix = for (x <- output) yield {
            val w = new Wire
            andGate(head, x, w)
            w
          }
          val suffix = for (x <- output) yield {
            val w = new Wire
            andGate(notHead, x, w)
            w
          }
          prefix ++ suffix
        }
      }
    }
    demux2(c)
  }

}

object Circuit extends CircuitSimulator {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  def andGateExample {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    in1.setSignal(true)
    run

    in2.setSignal(true)
    run
  }

  //
  // to complete with orGateExample and demuxExample...
  //
}

object CircuitMain extends App {
  // You can write tests either here, or better in the test class CircuitSuite.
  Circuit.andGateExample
}

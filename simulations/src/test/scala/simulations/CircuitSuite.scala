package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5
  
  ignore("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "and 3")
  }

  //
  // to complete with tests for orGate, demux, ...
  //
  ignore("demux") {
    val in0, in1,in2 = new Wire
    val out0, out1,out2,out3,out4,out5,out6,out7 = new Wire
    val c = List(in0,in1,in2)
  //  println(c.length)
    val out = List(out0,out1,out2,out3,out4,out5,out6,out7)
    val input = new Wire
    input.setSignal(true)
    demux(input,c,out)

    run
    out.foreach(x => print(x.getSignal+","))    
    assert(out(7).getSignal == true )
    
    in0.setSignal(true)
    in1.setSignal(true)
    in2.setSignal(true)
    run
    out.foreach(x => print(x.getSignal+","))
    assert(out(0).getSignal == true )
   
    in0.setSignal(false)
    in1.setSignal(true)
    in2.setSignal(false)
    run
    out.foreach(x => print(x.getSignal+","))
    assert(out(5).getSignal == true )   
    
    in0.setSignal(true)
    in1.setSignal(true)
    in2.setSignal(false)
    run
    out.foreach(x => print(x.getSignal+","))
    assert(out(1).getSignal == true )   
    
  }
  
  ignore ("empty demux" ) {
    val out0, input = new Wire
   input.setSignal(true)
    val out = List(out0)
    demux(input,Nil,out)
    run
    out.foreach(x => print(x.getSignal+","))
    assert(out(0).getSignal == input.getSignal)
  }

}

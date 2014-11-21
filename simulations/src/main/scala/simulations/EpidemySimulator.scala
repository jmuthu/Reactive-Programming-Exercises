package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  def uniqueRnd(sample: Int, max: Int, rndSet: Set[Int]): Set[Int] = {
    if (sample == rndSet.size)
      rndSet
    else
      uniqueRnd(sample, max, rndSet + randomBelow(max))
  }

  def isRndRate(rate: Double) = random < rate

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8
    val moveDayWindow: Int = 5

    val incubationTime = 6
    val dieTime = 14
    val immuneTime = 16
    val healTime = 18

    val prevalenceRate = 0.01
    val transRate = 0.4
    val dieRate = 0.25
    val airTravelRate = 0.01
    val vipRate = 0.05

    val infectedCount = (population * prevalenceRate ).toInt 
    val vipCount = (population * vipRate).toInt 

    val rnd = uniqueRnd(infectedCount, population, Set())
    val vip = uniqueRnd(vipCount + infectedCount, population, rnd)
  }

  import SimConfig._

  class Room(var personList: List[Person], val row: Int, val col: Int) {
    def isRoomVisiblyInfected = { personList.exists(p => (p.sick || p.dead)) }
    def isRoomInfected = { personList.exists(p => p.infected) }

    def addPerson(p: Person) {
      personList = personList + p
      p.row = row
      p.col = col
    }

    def removePerson(p: Person) {
      personList = personList.filter(x => x.id != p.id)
    }

    override def toString() = row + " " + col + personList
  }
  
  object Room {
   def apply(row: Int, col: Int) = new Room(Nil, row,col)
  }

  val rooms: Array[Array[Room]] = Array.tabulate(roomRows, roomColumns) { (r, c) => Room(r, c) }

  def getRoom(row: Int, col: Int) = rooms(row)(col) //rooms(row * roomRows + col)

  def getRndRoom = getRoom(randomBelow(roomRows), randomBelow(roomColumns))

  def getHealthyNeighbors(row: Int, col: Int): List[Room] = {
    val up = if (row == 0) getRoom((roomRows - 1), col) else getRoom((row - 1), col)
    val down = if (row == roomRows - 1) getRoom(0, col) else getRoom((row + 1), col)
    val left = if (col == 0) getRoom(row, (roomColumns - 1)) else getRoom(row, (col - 1))
    val right = if (col == roomColumns - 1) getRoom(row, 0) else getRoom(row, (col + 1))
    val neighbors = up :: down :: left :: right :: Nil
    for {
      room <- neighbors
      if (!room.isRoomVisiblyInfected)
    } yield room
  }

  val persons: List[Person] = for {
    i <- (0 to population - 1).toList
  } yield {
    val p = Person(i)
    p.moveToRoom(getRndRoom)
    if (rnd.exists(x => x == i)) p.infect
    else if (vip.exists(x => x == i)) p.immune = true
    p
  }

  class Person(val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    var row: Int = -1
    var col: Int = -1

    def moveToRoom(newRoom: Room) = {
      if (row != -1) {
        getRoom(row, col) removePerson this
        newRoom addPerson this
        transmitInfection()
      } else newRoom addPerson this

      afterDelay(randomBelow(moveDayWindow) + 1) { if (!dead) move() }
    }

    def move() {
      if (isRndRate(airTravelRate)) {
        moveToRoom(getRndRoom)
      } else {
        val healthyRooms = getHealthyNeighbors(row, col)
        if (healthyRooms != Nil) {
          val moveIndex = randomBelow(healthyRooms.length)
          moveToRoom(healthyRooms(moveIndex))
        }
      }
    }

    def transmitInfection() {
      if (!infected && !sick && !immune
        && getRoom(row, col).isRoomInfected
        && isRndRate(transRate)) {
        infect()
      }
    }

    def infect() {
      infected = true
      afterDelay(incubationTime) { sick = true }
      afterDelay(dieTime) {
        if (isRndRate(dieRate))
          dead = true
        else immunity()
      }
    }

    def immunity() {
      afterDelay(immuneTime - dieTime) {
        sick = false
        immune = true
      }
      afterDelay(healTime - dieTime) {
        infected = false
        immune = false
      }
    }

    override def toString() = id + "(" + row + "," + col + ") " + {
      if (immune) "Immune"
      else if (dead) "Dead"
      else if (sick) "Sick"
      else if (infected) "Infected"
      else "Healthy"
    }

  }
  
  object Person {
    def apply(i:Int) = new Person(i)
  }

}

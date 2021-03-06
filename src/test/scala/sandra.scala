//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

package sandra

import org.specs2.mutable._

class IntegrationTest extends Specification {
  private implicit val cluster = Cluster("TestCluster", new CassandraConfigurator("localhost:9160"))

  "Standard Family" should {

    object Model extends Keyspace("sandra_test") {
      object Family extends StandardFamily[Int, String]("family") {
        val column1 = StringColumn("column1")
        val column2 = StringColumn("column2")

        val columnB = BooleanColumn("columnB")
        val columnD = DateColumn("columnD")
        val columnI = IntColumn("columnI")
        val columnL = LongColumn("columnL")
        val columnS = StringColumn("columnS")
        val columnT = TimeUUIDColumn("columnT")
        val columnU = UUIDColumn("columnU")
      }
    }

    import Model._
    import Model.Family._

    val value1 = "value1"
    val value2 = "value2"


    "store and update" in {
      Family.truncate()
      Family.autoconf()

      Family.delete(1) must beEqualTo(true)
      Family.get(1)(Family.column1(_)) must beNone

      Family.column.update(1)(Family.column1(value1 + value2))
      Family.get(1)(Family.column1(_).get) must beEqualTo(Some(value1 + value2))

      Family.column.update(1)(Empty("column1"))

      Family.column.update(1)(StringColumn("column1").apply(value2))
      Family.get(1)(Family.column1(_).get) must beEqualTo(Some(value2))

      Family.update(1)(Family.column1(value1) :: Family.column2(value2) :: Nil)
      Family.get(1) { x => (Family.column1(x).get, Family.column2(x).get) } must 
        beEqualTo(Some((value1, value2)))

      Family.delete(1) must beEqualTo(true)
      Family.get(1)(Family.column1(_)) must beNone
    }

    "store and getAll and truncate" in {
      Family.truncate()
      Family.autoconf()

      (1 to 20).foreach(Family.column.update(_)(Family.column1(value1 + value2)))
      Family.getAll(10, false, None) must beEqualTo((1 to 10).toList)
      Family.getAll(10, false, Some(10)) must beEqualTo((11 to 20).toList)
      Family.truncate()
      Family.getAll(10, false, None) must beEqualTo(Nil)

      // DON'T WORK
      //Family.getAll(10, true, None) must beEqualTo(2 :: 1 :: Nil) // Need ordered partitioner to work
    }

    "store and getAll columns" in {
      Family.truncate()
      Family.autoconf()

      def values(x: Int, y: Int) = (x to y).map(i => "column" + i -> (i + 100))

      Family.update(100)(values(101, 120).map(x => IntColumn(x._1).apply(x._2)))

      Family.column.all[Int](100, 10, false, None) must beEqualTo(values(101, 110).toList)
      Family.column.all[Int](100, 10, false, Some("column110")) must beEqualTo(values(111, 120).toList)

      Family.column.all[Int](100, 10, true, None) must beEqualTo(values(111, 120).toList.reverse)
      Family.column.all[Int](100, 10, true, Some("column111")) must beEqualTo(values(101, 110).toList.reverse)

      Family.column.allNames(100, 10, false, None) must beEqualTo(values(101, 110).toList.map(_._1))
    }

    /*
    "support cassandra data type" in {
      val b = true
      val d = new java.util.Date
      val i: Int = 1
      val l: Long = 1
      val s = "hello, world"
      val t = new com.eaio.uuid.UUID
      val u = java.util.UUID.randomUUID

      Family.update(1)(
        Family.columnB(b) ::
        Family.columnD(d) ::
        Family.columnI(i) ::
        Family.columnL(l) ::
        Family.columnS(s) ::
        Family.columnT(t) ::
        Family.columnU(u) ::
        Nil)

      Family.get(1) { x => (
        Family.columnB(x).get,
        Family.columnD(x).get,
        Family.columnI(x).get,
        Family.columnL(x).get,
        Family.columnS(x).get,
        Family.columnT(x).get,
        Family.columnU(x).get
      ) } must beEqualTo(Some((
        b,
        d,
        i,
        l,
        s,
        t,
        u
      )))
    }
  */
  }
}


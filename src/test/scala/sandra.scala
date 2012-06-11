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

    Model.Family.autoconf()

    "store and update" in {
      Family.delete(1) must beEqualTo(true)
      Family.get(1)(Family.column1(_)) must beNone

      Family.columnUpdate(1)(Family.column1(value1 + value2))
      Family.get(1)(Family.column1(_).get) must beEqualTo(Some(value1 + value2))

      Family.update(1)(Family.column1(value1) :: Family.column2(value2) :: Nil)
      Family.get(1) { x => (Family.column1(x).get, Family.column2(x).get) } must 
        beEqualTo(Some((value1, value2)))

      Family.delete(1) must beEqualTo(true)
      Family.get(1)(Family.column1(_)) must beNone
    }

    "store and getAll" in {
      Family.columnUpdate(1)(Family.column1(value1 + value2))
      Family.columnUpdate(2)(Family.column1(value1 + value2))
      Family.getAll(10, false, None) must beEqualTo(1 :: 2 :: Nil)
      // DON'T WORK
      //Family.getAll(10, true, None) must beEqualTo(2 :: 1 :: Nil) // Need ordered partitioner to work
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


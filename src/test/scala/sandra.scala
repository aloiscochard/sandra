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
        val column1 = Column[String]("column1")
        val column2 = Column[String]("column2")
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
      true must beEqualTo(true)
    }
  }
}


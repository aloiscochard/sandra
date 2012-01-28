//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

package sandra
package example

object Model extends Keyspace("sandra_example") {
  object Users extends StandardFamily[Int, String]("users") {
    val name = StringColumn("name")
    val email = StringColumn("email")
  }
}

object UsersDAO {
  import Model._
  import Model.Users._

  case class User(name: String, email: String)

  def update(k: Int, u: User) =
    Users.update(k) { name(u.name) :: email(u.email) :: Nil }

  def get(k: Int) = Users.get(k) { x =>
    User(
      name(x).getOrElse("unknown"),
      email(x).getOrElse("")
    )
  }

  private implicit val cluster = Cluster("TestCluster", new CassandraConfigurator("localhost:9160"))

  Users.autoconf()
}

object App {
  def run {
    import UsersDAO._
    update(1, User("alois.cochard", "aloiscochard@gmail.com"))
    println(get(1))
    update(1, User("aloiscochard", "alois.cochard@gmail.com"))
    println(get(1))
  }
}

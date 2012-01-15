//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

package object sandra {
  type CassandraConfigurator = me.prettyprint.cassandra.service.CassandraHostConfigurator
  type HResult[T] = me.prettyprint.cassandra.service.template.ColumnFamilyResult[_, T]

  implicit object StringSerializer extends Serializer[String] {
    override type C = java.lang.String
    override def apply(x: String): java.lang.String = x
    override def cassandra = me.prettyprint.cassandra.serializers.StringSerializer.get
  }

  implicit object IntSerializer extends Serializer[Int] {
    override type C = java.lang.Integer
    override def apply(x: Int): java.lang.Integer = x
    override def cassandra = me.prettyprint.cassandra.serializers.IntegerSerializer.get
  }

  implicit def family2ddl[T <: Family](family: T)(implicit cluster: Cluster) =
    new hector.StandardFamilyDDL[T, T#K, T#N](cluster, family)

  implicit def family2template[T <: Family](family: T)(implicit cluster: Cluster) =
    new hector.StandardFamilyTemplate[T, T#K, T#N](cluster, family)
}

package sandra {
  case class Cluster(name: String, configurator: CassandraConfigurator)

  class Keyspace(val keyspaceName: String) {
    implicit val keyspace: Keyspace = this
  }

  trait Family {
    type K
    type N

    type CK = Serializer[K]#C
    type CN = Serializer[N]#C

    object Column {
      def apply[T <: String](name: String)(implicit family: Family) = StringColumn(name)
    }

    def familyName: String
    def keyspace: Keyspace

    def k: Serializer[K]
    def n: Serializer[N]
  }

  abstract class StandardFamily[KK, NN](val familyName: String)
        (implicit val k: Serializer[KK], val n: Serializer[NN], val keyspace: Keyspace)
      extends Family {
    override type K = KK
    override type N = NN
    implicit val family: Family = this

  }

  sealed trait Column[T, N] {
    type Name = N
    def name: N
    def apply(result: HResult[N]): Option[T]
    def apply(value: T): ColumnValue[T, N]
    def serializer: Serializer[T]
  }

  case class StringColumn[N](name: N) extends Column[String, N] {
    override def apply(result: HResult[N]) = Option(result.getString(name))
    override def apply(value: String) = new ColumnValue[String, N](this, value)
    override val serializer = StringSerializer
  }

  final class ColumnValue[T, N](val column: Column[T, N], value: T) {
    def apply() = value
  }
  
  trait Serializer[T] {
    type C
    def apply(t: T): C
    def cassandra: me.prettyprint.cassandra.serializers.AbstractSerializer[C]
  }

}

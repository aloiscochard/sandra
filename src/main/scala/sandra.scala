//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

import scala.util.control.Exception._

import java.util.Date;
import java.util.UUID;
import com.eaio.uuid.{UUID => TimeUUID};

// TODO [aloiscochard] Complete composite support with own type, like Composite[(A, B)], then accept tuple

package object sandra {
  type CassandraConfigurator = me.prettyprint.cassandra.service.CassandraHostConfigurator
  type HResult[T] = me.prettyprint.cassandra.service.template.ColumnFamilyResult[_, T]

  def Empty[N](name: N) = ArrayByteColumn(name)(ArrayByteSerializer).apply(Array(0: Byte))

  implicit object ArrayByteSerializer extends Serializer[Array[Byte]] {
    override type C = Array[Byte]
    override def apply(x: Array[Byte]): Array[Byte] = x
    override def cassandra = me.prettyprint.cassandra.serializers.BytesArraySerializer.get
  }

  implicit object BooleanSerializer extends Serializer[Boolean] {
    override type C = java.lang.Boolean
    override def apply(x: Boolean): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.BooleanSerializer.get
  }

  implicit object DateSerializer extends Serializer[Date] {
    override type C = Date
    override def apply(x: Date): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.DateSerializer.get
  }

  implicit object IntSerializer extends Serializer[Int] {
    override type C = java.lang.Integer
    override def apply(x: Int): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.IntegerSerializer.get
  }

  implicit object LongSerializer extends Serializer[Long] {
    override type C = java.lang.Long
    override def apply(x: Long): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.LongSerializer.get
  }

  implicit object StringSerializer extends Serializer[String] {
    override type C = java.lang.String
    override def apply(x: String): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.StringSerializer.get
  }

  implicit object TimeUUIDSerializer extends Serializer[TimeUUID] {
    override type C = TimeUUID
    override def apply(x: TimeUUID): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.TimeUUIDSerializer.get
  }

  implicit object UUIDSerializer extends Serializer[UUID] {
    override type C = UUID
    override def apply(x: UUID): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.UUIDSerializer.get
  }


  // TODO [aloiscochard] Improve with scala-ish composite/dynacomposite types
  import me.prettyprint.hector.api.beans.Composite
  import me.prettyprint.hector.api.beans.DynamicComposite

  implicit object CompositeSerializer extends Serializer[Composite] {
    override type C = Composite
    override def apply(x: Composite): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.CompositeSerializer.get
  }

  implicit object DynamicCompositeSerializer extends Serializer[DynamicComposite] {
    override type C = DynamicComposite
    override def apply(x: DynamicComposite): C = x
    override def cassandra = me.prettyprint.cassandra.serializers.DynamicCompositeSerializer.get
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

    def familyName: String
    def keyspace: Keyspace

    def strategyClass: String
    def replicationFactor: Int
    def describe(definition: me.prettyprint.hector.api.ddl.ColumnFamilyDefinition): me.prettyprint.hector.api.ddl.ColumnFamilyDefinition

    def k: Serializer[K]
    def n: Serializer[N]
  }

  abstract class StandardFamily[KK, NN](val familyName: String)
        (implicit val k: Serializer[KK], val n: Serializer[NN], val keyspace: Keyspace)
      extends Family {
    override type K = KK
    override type N = NN
    implicit val family: Family = this

    override val strategyClass = me.prettyprint.cassandra.service.ThriftKsDef.DEF_STRATEGY_CLASS
    override val replicationFactor = 1
    override def describe(definition:  me.prettyprint.hector.api.ddl.ColumnFamilyDefinition) = definition
  }

  sealed trait Column[T, N] {
    type Name = N

    final def apply(result: HResult[N]): Option[T] =
      catching(classOf[NullPointerException]).opt(read(result)).getOrElse(None)

    def name: N
    def apply(value: T): ColumnValue[T, N]
    def serializer: Serializer[T]
    def read(result: HResult[N]): Option[T]
  }

  case class ArrayByteColumn[N](name: N)
      (implicit override val serializer: Serializer[Array[Byte]])
      extends Column[Array[Byte], N] {
    override def apply(value: Array[Byte]) = new ColumnValue[Array[Byte], N](this, value)
    override def read(result: HResult[N]) = Option(result.getByteArray(name))
  }

  case class BooleanColumn[N](name: N)
      (implicit override val serializer: Serializer[Boolean])
      extends Column[Boolean, N] {
    override def apply(value: Boolean) = new ColumnValue[Boolean, N](this, value)
    override def read(result: HResult[N]) = Option(result.getBoolean(name))
  }

  case class DateColumn[N](name: N)
      (implicit override val serializer: Serializer[Date])
      extends Column[Date, N] {
    override def apply(value: Date) = new ColumnValue[Date, N](this, value)
    override def read(result: HResult[N]) = Option(result.getDate(name))
  }

  case class IntColumn[N](name: N)
      (implicit override val serializer: Serializer[Int])
      extends Column[Int, N] {
    override def apply(value: Int) = new ColumnValue[Int, N](this, value)
    override def read(result: HResult[N]) = Option(result.getInteger(name))
  }

  case class LongColumn[N](name: N)
      (implicit override val serializer: Serializer[Long])
      extends Column[Long, N] {
    override def apply(value: Long) = new ColumnValue[Long, N](this, value)
    override def read(result: HResult[N]) = Option(result.getLong(name))
  }

  case class StringColumn[N](name: N)
      (implicit override val serializer: Serializer[String])
      extends Column[String, N] {
    override def apply(value: String) = new ColumnValue[String, N](this, value)
    override def read(result: HResult[N]) = Option(result.getString(name))
  }

  case class TimeUUIDColumn[N](name: N)
      (implicit override val serializer: Serializer[TimeUUID])
      extends Column[TimeUUID, N] {
    override def apply(value: TimeUUID) = new ColumnValue[TimeUUID, N](this, value)
    override def read(result: HResult[N]) = Option(new TimeUUID(result.getUUID(name).toString))
  }

  case class UUIDColumn[N](name: N)
      (implicit override val serializer: Serializer[UUID])
      extends Column[UUID, N] {
    override def apply(value: UUID) = new ColumnValue[UUID, N](this, value)
    override def read(result: HResult[N]) = Option(result.getUUID(name))
  }

  final class ColumnValue[T, N](val column: Column[T, N], value: T) {
    def apply(): T = value
  }
  
  trait Serializer[T] {
    type C
    def apply(t: T): C
    def cassandra: me.prettyprint.cassandra.serializers.AbstractSerializer[C]
  }

}

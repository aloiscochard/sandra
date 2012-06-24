//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

package sandra
package hector

// TODO [aloiscochard] Handle exception with validation, and go lazy with IO ?
// TODO [aloiscochard] Use HList for type safety!
import collection.JavaConversions._
import me.prettyprint.hector.api.factory.HFactory

sealed trait Hector[T <: Family, K, N] {
  val cluster: Cluster
  val family: T

  val hcluster = HFactory.getOrCreateCluster(cluster.name, cluster.configurator)
  val hkeyspace = cache.keyspace(cluster, family.keyspace)
  val htemplate = cache.template[K, N](hkeyspace, family)
}

final class StandardFamilyTemplate[T <: Family, K, N]
      (override val cluster: Cluster, override val family: T)
    extends Hector[T, K, N] {

  val column = new ColumnHector

  def delete(key: family.type#K): Boolean = {
    htemplate.deleteRow(family.k(key))
    true
  }


  def get[R](key: family.type#K)(f: HResult[family.type#N] => R): Option[R] = {
    val result = htemplate.queryColumns(family.k(key))
      .asInstanceOf[HResult[family.type#N]]
    if (result.hasResults)
      Some(f(result))
    else None
  }

  def getAll(_size: Int, reversed: Boolean = false, from: Option[family.type#K] = None): List[family.type#K] = {
    val size = from.map(_ => _size +1).getOrElse(_size)
    val query = HFactory.createRangeSlicesQuery(
      hkeyspace, 
      family.k.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[family.type#K]],
      family.n.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[family.type#N]],
      family.k.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[family.type#K]])
        .setColumnFamily(family.familyName)
        .setRange(null.asInstanceOf[family.type#N], null.asInstanceOf[family.type#N], reversed, size)
        .setRowCount(size);


    from.foreach(query.setKeys(_, null.asInstanceOf[family.type#K]))

    var keys = List[family.type#K]()
    val i = query.execute().get().iterator()
    while(i.hasNext()) {
      val row = i.next()
      val key = row.getKey()

      if(from.map(key != _).getOrElse(true)) keys ::= key.asInstanceOf[family.type#K]
    }

    keys.reverse
  }

  // TODO [aloiscochard] Replace with shapeless
  //def update(key: family.type#K)(values: => Seq[ColumnValue[_, family.type#N]]): this.type = {
  def update(key: family.type#K)(values: => Seq[ColumnValue[_, _]]) = {
    val updater = htemplate.createUpdater(family.k(key))
    values.foreach { value =>
      updater.setValue(value.column.name, value(), value.column.serializer.cassandra
          .asInstanceOf[me.prettyprint.hector.api.Serializer[Any]])
    }
    htemplate.update(updater)
    key
  }

  class ColumnHector {
    import me.prettyprint.hector.api.beans.HColumn

    def delete[C <: Column[_, N]](key: family.type#K, c: C): Boolean =
      delete(key, c.name)

    def delete(key: family.type#K, name: N): Boolean = {
      htemplate.deleteColumn(family.k(key), name)
      true
    }

    def get[T](key: family.type#K)(column: Column[T, family.type#N]): Option[T] =
      StandardFamilyTemplate.this.get(key)(x => column(x)).flatMap(identity)

    def update(key: family.type#K)(value: ColumnValue[_, family.type#N]) = {
      StandardFamilyTemplate.this.update(key)(value :: Nil)
      key
    }

    def allNames
      (key: family.type#K, size: Int, reversed: Boolean = false, from: Option[family.type#N] = None): List[family.type#N] =
        _all[Array[Byte], family.type#N](key, size, reversed, from)(_.getName)(ArrayByteSerializer)

    def all[V](key: family.type#K, size: Int, reversed: Boolean = false, from: Option[family.type#N] = None)
      (implicit serializer: Serializer[V]): List[(family.type#N, V)] =
        _all[V, (family.type#N, V)](key, size, reversed, from)(x => (x.getName, x.getValue))

    private def _all[V, T](key: family.type#K, _size: Int, reversed: Boolean, from: Option[family.type#N])
        (f: HColumn[family.type#N, V] => T)
        (implicit serializer: Serializer[V]): List[T] = {
      val size = from.map(_ => _size +1).getOrElse(_size)
      val query = HFactory.createSliceQuery(
        hkeyspace, 
        family.k.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[family.type#K]],
        family.n.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[family.type#N]],
        serializer.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[V]])
          .setColumnFamily(family.familyName)
          .setRange(from.getOrElse(null.asInstanceOf[family.type#N]), null.asInstanceOf[family.type#N], reversed, size)
          .setKey(key)

      val columns = query.execute().get().getColumns().toList
      from.map(_ => columns.tail).getOrElse(columns).map(f)
    }
  }
}

final class StandardFamilyDDL[T <: Family, K, N]
      (override val cluster: Cluster, override val family: T)
    extends Hector[T, K, N] {

  def autoconf(): Boolean = {
    val ksname = family.keyspace.keyspaceName
    val ksdef = Option(hcluster.describeKeyspace(ksname)).getOrElse {
      val d = HFactory.createKeyspaceDefinition(ksname, family.strategyClass, family.replicationFactor, Nil)
      hcluster.addKeyspace(d, true)
      d
    }
    if (ksdef.getCfDefs.map(_.getName).find(_ == family.familyName).isEmpty) {
      val d = HFactory.createColumnFamilyDefinition(ksname, family.familyName)
      hcluster.addColumnFamily(family.describe(d))
    }
    true
  }
  //createKeyspaceDefinition(String keyspaceName, String strategyClass, int replicationFactor, List<ColumnFamilyDefinition> cfDefs)

  def truncate(): Boolean = {
    hcluster.truncate(family.keyspace.keyspaceName, family.familyName)
    true
  }
    
}

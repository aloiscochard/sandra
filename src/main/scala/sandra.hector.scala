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
  protected val hcluster = HFactory.getOrCreateCluster(cluster.name, cluster.configurator)
  protected val hkeyspace = cache.keyspace(cluster, family.keyspace)
  protected val htemplate = cache.template[K, N](hkeyspace, family)
}

final class StandardFamilyTemplate[T <: Family, K, N]
      (override val cluster: Cluster, override val family: T)
    extends Hector[T, K, N] {

  def delete(key: family.type#K): Boolean = {
    htemplate.deleteRow(family.k(key))
    true
  }

  def columnDelete[C <: Column[_, N]](key: family.type#K, c: C): Boolean =
    columnDelete(key, c.name)

  def columnDelete(key: family.type#K, name: N): Boolean = {
    htemplate.deleteColumn(family.k(key), name)
    true
  }

  def columnNames(key: family.type#K): Seq[N] =
    htemplate.queryColumns(family.k(key)).getColumnNames.toList.asInstanceOf[Seq[N]]

  def columnUpdate(key: family.type#K)(value: ColumnValue[_, family.type#N]): this.type = update(key)(value :: Nil)

  def get[R](key: family.type#K)(f: HResult[family.type#N] => R): Option[R] = {
    val result = htemplate.queryColumns(family.k(key))
      .asInstanceOf[HResult[family.type#N]]
    if (result.hasResults)
      Some(f(result))
    else None
  }

  def update(key: family.type#K)(values: => Seq[ColumnValue[_, family.type#N]]): this.type = {
    val updater = htemplate.createUpdater(family.k(key))
    values.foreach { value =>
      updater.setValue(value.column.name, value(), value.column.serializer.cassandra
          .asInstanceOf[me.prettyprint.hector.api.Serializer[Any]])
    }
    htemplate.update(updater)
    this
  }

}

final class StandardFamilyDDL[T <: Family, K, N]
      (override val cluster: Cluster, override val family: T)
    extends Hector[T, K, N] {

  def autoconf() : Boolean = {
    // TODO [aloiscochard] Autogenerate column descripton (by adding parameters Seq[Column[_,_]]
    val ksname = family.keyspace.keyspaceName
    val ksdef = Option(hcluster.describeKeyspace(ksname)).getOrElse {
      val d = HFactory.createKeyspaceDefinition(ksname)
      hcluster.addKeyspace(d, true)
      d
    }
    if (ksdef.getCfDefs.map(_.getName).find(_ == family.familyName).isEmpty) {
      hcluster.addColumnFamily(
        HFactory.createColumnFamilyDefinition(ksname, family.familyName))
    }
    true
  }
    
}

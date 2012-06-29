//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

package sandra.hector

import scala.collection.JavaConverters._

import sandra._

import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.{Keyspace => HKeyspace}
import me.prettyprint.hector.api.{Cluster => HCluster}
import me.prettyprint.cassandra.service.template.{ColumnFamilyTemplate => FamilyTemplate}
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import me.prettyprint.cassandra.service.FailoverPolicy
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;

package object cache {
  def keyspace(hcluster: HCluster, cluster: Cluster, keyspace: Keyspace): HKeyspace = keyspaces.synchronized {
    val key = cluster -> keyspace
    keyspaces.getOrElse(key, {

      val k = HFactory.createKeyspace(
        keyspace.keyspaceName,
        hcluster
      )
      /*
        new QuorumAllConsistencyLevelPolicy,
        FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE,
        credentials.asJava
      )
      */
      keyspaces += (key -> k)
      k
    })
  }

  def template[K, N]
      (keyspace: HKeyspace, family: Family): FamilyTemplate[Family#CK, Any] =
    templates.synchronized {
      val key = keyspace -> family
      templates.getOrElse(key, {
        val t = new ThriftColumnFamilyTemplate[Family#CK, Family#CN](keyspace, family.familyName,
          family.k.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[Family#CK]],
          family.n.cassandra.asInstanceOf[me.prettyprint.hector.api.Serializer[Family#CN]])
        templates += (key -> t)
        t
      }).asInstanceOf[FamilyTemplate[Family#CK, Any]]
    }


  private var keyspaces = Map[(Cluster, Keyspace), HKeyspace]()
  private var templates = Map[(HKeyspace, Family), FamilyTemplate[_, _]]()
}

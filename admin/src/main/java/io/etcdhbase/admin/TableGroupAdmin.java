package io.etcdhbase.admin;

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** Minimal synchronous boundary used by the idempotent table initializer. */
interface TableGroupAdmin {
  List<String> listNamespaces() throws Exception;

  void createNamespace(String namespace) throws Exception;

  List<TableName> listTableNames(String namespace) throws Exception;

  TableDescriptor getDescriptor(TableName tableName) throws Exception;

  List<RegionInfo> getRegions(TableName tableName) throws Exception;

  boolean isEmpty(TableName tableName) throws Exception;

  void createTable(TableDescriptor descriptor, byte[][] splitKeys) throws Exception;
}

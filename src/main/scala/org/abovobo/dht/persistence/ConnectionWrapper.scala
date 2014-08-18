/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

package org.abovobo.dht.persistence

import java.sql.{Array => _, _}
import java.util
import java.util.Properties
import java.util.concurrent.Executor

/**
 * This class defines wrapper over the java.sql.Connection allowing to use it in our hierarchy.
 *
 * @param connection Actual [[java.sql.Connection]] instance
 */
class ConnectionWrapper(val connection: Connection) extends Connection {

  /// Ensure that we won't wrap already wrapped connection
  require(!this.connection.isInstanceOf[ConnectionWrapper])

  /**
   * Override close so it really does nothing
   */
  override def close(): Unit = Unit

  /**
   * This method actually closes the connection.
   */
  def dispose(): Unit = this.connection.close()

  override def createStatement(): Statement = this.connection.createStatement()
  override def setAutoCommit(autoCommit: Boolean): Unit = this.connection.setAutoCommit(autoCommit)
  override def setHoldability(holdability: Int): Unit = this.connection.setHoldability(holdability)
  override def clearWarnings(): Unit = this.connection.clearWarnings()
  override def getNetworkTimeout: Int = this.connection.getNetworkTimeout
  override def createBlob(): Blob = this.connection.createBlob()
  override def createSQLXML(): SQLXML = this.connection.createSQLXML()
  override def setSavepoint(): Savepoint = this.connection.setSavepoint()
  override def setSavepoint(name: String): Savepoint = this.connection.setSavepoint(name)
  override def createNClob(): NClob = this.connection.createNClob()
  override def getTransactionIsolation: Int = this.connection.getTransactionIsolation
  override def getClientInfo(name: String): String = this.connection.getClientInfo(name)
  override def getClientInfo: Properties = this.connection.getClientInfo
  override def getSchema: String = this.connection.getSchema
  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = this.connection.setNetworkTimeout(executor, milliseconds)
  override def getMetaData: DatabaseMetaData = this.connection.getMetaData
  override def getTypeMap: util.Map[String, Class[_]] = this.connection.getTypeMap
  override def rollback(): Unit = this.rollback()
  override def rollback(savepoint: Savepoint): Unit = this.rollback(savepoint)
  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = this.createStatement(resultSetType, resultSetConcurrency)
  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = this.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
  override def getHoldability: Int = this.connection.getHoldability
  override def setReadOnly(readOnly: Boolean): Unit = this.connection.setReadOnly(readOnly)
  override def setClientInfo(name: String, value: String): Unit = this.connection.setClientInfo(name, value)
  override def setClientInfo(properties: Properties): Unit = this.connection.setClientInfo(properties)
  override def isReadOnly: Boolean = this.connection.isReadOnly
  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = this.connection.setTypeMap(map)
  override def getCatalog: String = this.connection.getCatalog
  override def createClob(): Clob = this.connection.createClob()
  override def setTransactionIsolation(level: Int): Unit = this.connection.setTransactionIsolation(level)
  override def nativeSQL(sql: String): String = this.connection.nativeSQL(sql)
  override def prepareCall(sql: String): CallableStatement = this.prepareCall(sql)
  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement = this.prepareCall(sql, resultSetType, resultSetConcurrency)
  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement = this.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
  override def createArrayOf(typeName: String, elements: Array[AnyRef]): java.sql.Array = this.connection.createArrayOf(typeName, elements)
  override def setCatalog(catalog: String): Unit = this.connection.setCatalog(catalog)
  override def getAutoCommit: Boolean = this.connection.getAutoCommit
  override def abort(executor: Executor): Unit = this.connection.abort(executor)
  override def isValid(timeout: Int): Boolean = this.connection.isValid(timeout)
  override def prepareStatement(sql: String): PreparedStatement = this.connection.prepareStatement(sql)
  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency)
  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): PreparedStatement = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = this.connection.prepareStatement(sql, autoGeneratedKeys)
  override def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement = this.connection.prepareStatement(sql, columnIndexes)
  override def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement = this.connection.prepareStatement(sql, columnNames)
  override def releaseSavepoint(savepoint: Savepoint): Unit = releaseSavepoint(savepoint)
  override def isClosed: Boolean = this.connection.isClosed
  override def createStruct(typeName: String, attributes: Array[AnyRef]): Struct = this.connection.createStruct(typeName, attributes)
  override def getWarnings: SQLWarning = this.connection.getWarnings
  override def setSchema(schema: String): Unit = this.connection.setSchema(schema)
  override def commit(): Unit = this.connection.commit()
  override def unwrap[T](iface: Class[T]): T = this.connection.unwrap(iface)
  override def isWrapperFor(iface: Class[_]): Boolean = this.connection.isWrapperFor(iface)
}

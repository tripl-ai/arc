package ai.tripl.arc.util

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.Locale
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

class JDBCSink(url: String, connectionProperties: Properties) extends ForeachWriter[Row] {
  var conn: Connection = _
  var dialect: JdbcDialect = _
  var stmt: PreparedStatement = _
  var supportsTransactions = false
  var rowCount = 0

  def open(partitionId: Long, version: Long): Boolean = {
    dialect = JdbcDialects.get(url)
    conn = DriverManager.getConnection(url, connectionProperties)

    val metadata = conn.getMetaData
    supportsTransactions = metadata.supportsTransactions()
    if (supportsTransactions) {
      conn.setAutoCommit(false) // Everything in the same db transaction.
    }

    true
  }

  def process(row: Row): Unit = {
    val schema = row.schema
    val dbtable = connectionProperties.asScala.getOrElse("dbtable", "")

    if (stmt == null) {
      val columns = schema.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
      val placeholders = schema.map(_ => "?").mkString(",")
      val insertStatement = s"""INSERT INTO ${dbtable} (${columns}) VALUES ($placeholders)"""
      stmt = conn.prepareStatement(insertStatement)
      rowCount = 0
    }


    val setters = schema.fields.map(f => makeSetter(conn, dialect, f.dataType))
    val nullTypes = schema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val numFields = schema.fields.length

    var i = 0
    while (i < numFields) {
      if (row.isNullAt(i)) {
        stmt.setNull(i + 1, nullTypes(i))
      } else {
        setters(i).apply(stmt, row, i)
      }
      i = i + 1
    }
    stmt.addBatch
    rowCount += 1
  }

  def close(errorOrNull: Throwable): Unit = {
    var committed = false
    try {
      if (rowCount > 0) {
        stmt.executeBatch

        if (supportsTransactions) {
          conn.commit
        }
        committed = true
      }
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback
        }
        conn.close
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close
        } catch {
          case e: Exception => throw new Exception("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

  // this code is taken from the org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils package:

  // A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
  // `PreparedStatement`. The last argument `Int` means the index for the value to be set
  // in the SQL statement and also used for the value in `Row`.
  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  private def makeSetter(
      conn: Connection,
      dialect: JdbcDialect,
      dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

}



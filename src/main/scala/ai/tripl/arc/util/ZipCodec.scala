package ai.tripl.arc.util

import java.io.{OutputStream, BufferedInputStream, InputStream}
import java.util.zip._

import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.compress.zlib._

class ZipDecompressor(nowrap: Boolean) extends BuiltInZlibInflater(nowrap) {}

class ZipDecompressorStream(inputStream: InputStream, decompressor: Decompressor) extends CompressionInputStream(inputStream) {
  val zipInputStream = new ZipInputStream(new BufferedInputStream(inputStream))

  override def read() = {
    zipInputStream.read()
  }

  override def read(b: Array[Byte], off: Int, len: Int) = {
    var bytesRead = zipInputStream.read(b, off, len)
    if (bytesRead == -1) {
      zipInputStream.getNextEntry()
      bytesRead = zipInputStream.read(b, off, len)
    }

    bytesRead
  }

  override def resetState() = {
    decompressor.reset()
  }

  override def close() = {
    if (zipInputStream != null) {
      zipInputStream.closeEntry()
      zipInputStream.close()
    }
  }
}

class ZipCompressor(level: Int, nowrap: Boolean) extends BuiltInZlibDeflater(level, nowrap) {}

class ZipCompressorStream(outputStream: OutputStream, compressor: Compressor) extends CompressionOutputStream(outputStream) {
  val zipOutputStream = new ZipOutputStream(outputStream)
  zipOutputStream.putNextEntry(new ZipEntry("zipEntry"))

  override def write(b: Int) = {
    zipOutputStream.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = {
    zipOutputStream.write(b, off, len)
  }

  override def resetState() = compressor.reset()

  override def close() = {
    zipOutputStream.close()
  }

  override def finish() = {
    zipOutputStream.closeEntry()
    zipOutputStream.finish()
  }
}

class ZipCodec extends CompressionCodec {
  // register extension
  override def getDefaultExtension(): String = ".zip"

  // input
  override def getDecompressorType(): Class[_ <: org.apache.hadoop.io.compress.Decompressor] = classOf[ZipDecompressor]

  override def createDecompressor(): Decompressor = new ZipDecompressor(false)

  override def createInputStream(in: InputStream): CompressionInputStream = {
    new ZipDecompressorStream(in, createDecompressor())
  }

  override def createInputStream(inputStream: InputStream, decompressor: Decompressor): CompressionInputStream = {
    new ZipDecompressorStream(inputStream, decompressor)
  }

  // output
  override def getCompressorType(): Class[_ <: org.apache.hadoop.io.compress.Compressor] = classOf[ZipCompressor]

  override def createCompressor(): Compressor = new ZipCompressor(Deflater.DEFAULT_COMPRESSION, false)

  override def createOutputStream(outputStream: OutputStream): CompressionOutputStream = {
    new ZipCompressorStream(outputStream, createCompressor())
  }

  override def createOutputStream(outputStream: OutputStream, compressor: Compressor): CompressionOutputStream = {
    new ZipCompressorStream(outputStream, compressor)
  }
}
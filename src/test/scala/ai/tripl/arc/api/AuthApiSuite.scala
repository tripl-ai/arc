package ai.tripl.arc.api

import org.scalatest.FunSuite

import API._

class AuthApiSuite extends FunSuite {

    test("Read encryption type correctly for Amazon S3") {

        assert(AmazonS3EncryptionType.fromString("SSE-KMS") == Some(AmazonS3EncryptionType.SSE_KMS))
        assert(AmazonS3EncryptionType.fromString("SSE-S3") == Some(AmazonS3EncryptionType.SSE_S3))
        assert(AmazonS3EncryptionType.fromString("SSE-C") == Some(AmazonS3EncryptionType.SSE_C))

        assert(AmazonS3EncryptionType.fromString("sse-kms") == Some(AmazonS3EncryptionType.SSE_KMS))
        assert(AmazonS3EncryptionType.fromString(" sse-kms ") == Some(AmazonS3EncryptionType.SSE_KMS))

        assert(AmazonS3EncryptionType.fromString("") == None)
        assert(AmazonS3EncryptionType.fromString(null) == None)

    }

}
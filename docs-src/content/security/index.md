---
date: 2016-03-09T00:11:02+01:00
title: Security
weight: 85
type: blog
---

## Encryption

### Arc Local

Spark [natively supports](https://spark.apache.org/docs/latest/security.html) many different types of encryption. When running as a single master from the `Dockerfile` (as per [Arc Starter](https://github.com/tripl-ai/arc-starter)) then set these options to ensure temporary data spilled to disk and any network traffic will be encrypted with a randomly generated key for each execution:

```bash
--conf spark.authenticate=true \
--conf spark.authenticate.secret=$(openssl rand -hex 64) \
--conf spark.io.encryption.enabled=true \
--conf spark.network.crypto.enabled=true \
```

### Arc Jupyter

The [Arc Local](#arclocal) encrpytion options are also set in [Arc Jupyter](https://github.com/tripl-ai/arc-jupyter) and have a secure random secret generated for each notebook session and cannot be overridden by setting custom configurations.

## Authentication

The `authentication` object defines the authentication parameters for connecting to a remote service (e.g. HDFS, Blob Storage, etc.). To define these the `authentication` key can be supplied for different providers:

```json
{
  "authentication": {
    "method": "AmazonAccessKey",
    "accessKeyID": "AKIAIOSFODNN7EXAMPLE",
    "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  }
}
```

It is strongly discouraged to use simple authentication like above and in favor of mechanisms like `AmazonIAM` which do not risk exposing secrets.

{{< note title="Authentication Scope" >}}
Currently these options are defined at a global level meaning that if an `authentication` object is supplied for one stage it will apply to all stages after that.

Amazon Web Services `s3a` access is an exception which has `stage` level scoping of permissions which override globals.
{{</note>}}

### Parameters

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
|method|String|true|A value of `AzureSharedKey`, `AzureSharedAccessSignature`, `AzureDataLakeStorageToken`, `AzureDataLakeStorageGen2AccountKey`, `AzureDataLakeStorageGen2OAuth`, `AmazonAccessKey`, `AmazonAnonymous`, `AmazonIAM`, `AmazonEnvironmentVariable`, `GoogleCloudStorageKeyFile` which defines which method should be used to authenticate with the remote service.|
|accountName|String|false*|Required for `AzureSharedKey` and `AzureSharedAccessSignature`.|
|signature|String|false*|Required for `AzureSharedKey`.|
|container|String|false*|Required for `AzureSharedAccessSignature`.|
|token|String|false*|Required for `AzureSharedAccessSignature`.|
|clientID|String|false*|Required for `AzureDataLakeStorageToken`.|
|refreshToken|String|false*|Required for `AzureDataLakeStorageToken`.|
|accountName|String|false*|Required for `AzureDataLakeStorageGen2AccountKey`.|
|accessKey|String|false*|Required for `AzureDataLakeStorageGen2AccountKey`.|
|clientID|String|false*|Required for `AzureDataLakeStorageGen2OAuth`.|
|secret|String|false*|Required for `AzureDataLakeStorageGen2OAuth`.|
|directoryID|String|false*|Required for `AzureDataLakeStorageGen2OAuth`.|
|accessKeyID|String|false*|Required for `AmazonAccessKey`.|
|secretAccessKey|String|false*|Required for `AmazonAccessKey`.|
|accessKeyID|String|false*|Required for `AmazonIAM`.|
|secretAccessKey|String|false*|Required for `AmazonAccessKey`.|
|encryptionAlgorithm|String|false*|The bucket encrpytion algorithm: `SSE-S3`, `SSE-KMS`, `SSE-C`. Optional for `AmazonIAM`.|
|kmsArn|String|false*|The Key Management Service Amazon Resource Name when using `SSE-KMS` encryptionAlgorithm e.g. `arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab`. Optional for `AmazonIAM`.|
|customKey|String|false*|The key to use when using Customer-Provided Encryption Keys (`SSE-C`). Optional for `AmazonIAM`.|
|endpoint|String|false|Used for setting S3 endpoint for services like `Ceph Object Store` or `Minio`. Optional for `AmazonAccessKey`.|
|sslEnabled|Boolean|false|Used to set whether to use SSL. Optional for `AmazonAccessKey`.|
|projectID|String|false*|Required for `GoogleCloudStorageKeyFile`.|
|keyFilePath|String|false*|Required for `GoogleCloudStorageKeyFile`.|

### Examples

```json
{
    "type": "DelimitedExtract",
    ...
    "authentication": {
        "method": "AzureSharedKey",
        "accountName": "myaccount",
        "signature": "ctzMq410TV3wS7upTBcunJTDLEJwMAZuFPfr0mrrA08=",
    }
    ...
}
```

```json
{
    "type": "DelimitedExtract",
    ...
    "authentication": {
        "method": "AzureSharedAccessSignature",
        "accountName": "myaccount",
        "container": "mycontainer",
        "token": "sv=2015-04-05&st=2015-04-29T22%3A18%3A26Z&se=2015-04-30T02%3A23%3A26Z&sr=b&sp=rw&sip=168.1.5.60-168.1.5.70&spr=https&sig=Z%2FRHIX5Xcg0Mq2rqI3OlWTjEg2tYkboXr1P9ZUXDtkk%3D",
    }
    ...
}
```

```json
{
    "type": "DelimitedExtract",
    ...
    "authentication": {
        "method": "AmazonAccessKey",
        "accessKeyID": "AKIAIOSFODNN7EXAMPLE",
        "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "endpoint": "http://minio:9000"
    }
    ...
}
```

## Amazon Web Services

### Authentication

When running on Amazon Web Services Arc will try to resolve permissions in this order. These can ben overridden for a specific stage by specifying a [authentication](../security/#authentication) method.

- `SimpleAWSCredentialsProvider`: access key and secret
- `EnvironmentVariableCredentialsProvider`: environment variables of access key and secret
- `InstanceProfileCredentialsProvider`: IAM Role attached to the EC2 instance
- `ContainerCredentialsProvider`: IAM Role attached to the container in case of ECS and EKS
- `AnonymousAWSCredentialsProvider`: try to access without credentials - useful for accessing the [Registry of Open Data on AWS](https://registry.opendata.aws/).

### Encryption-at-Rest

[Amazon S3](https://aws.amazon.com/s3/) supports full encryption for data-at-rest via the [Amazon Key Management Service](https://aws.amazon.com/kms/). When used with Amazon [Identity and Access Management](https://aws.amazon.com/iam/) it provides a mechanism for securely storing data and providing access control that works seamlessly with Arc.

A policy like this will work:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:kms:example-region-1:123456789012:key/example-key-id",
        "arn:aws:s3:::example-bucket-name/*"
      ]
    }
  ]
}
```

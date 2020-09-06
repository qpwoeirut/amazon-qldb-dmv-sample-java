/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.qldb.tutorial

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder
import com.amazonaws.services.identitymanagement.model.*
import com.amazonaws.services.qldb.AmazonQLDB
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder
import com.amazonaws.services.qldb.model.*
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.DescribeJournalExport.describeExport
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

/**
 * Export a journal to S3.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 *
 * This code requires an S3 bucket. You can provide the name of an S3 bucket that
 * you wish to use via the arguments (args[0]). The code will check if the bucket
 * exists and create it if not. If you don't provide a bucket name, the code will
 * create a unique bucket for the purposes of this tutorial.
 *
 * Optionally, you can provide an IAM role ARN to use for the journal export via
 * the arguments (args[1]). Otherwise, the code will create and use a role named
 * "QLDBTutorialJournalExportRole".
 *
 * S3 Export Encryption:
 * Optionally, you can provide a KMS key ARN to use for S3-KMS encryption, via
 * the arguments (args[2]). The tutorial code will fail if you provide a KMS key
 * ARN that doesn't exist.
 *
 * If KMS Key ARN is not provided, the Tutorial Code will use
 * SSE-S3 for the S3 Export.
 *
 * If provided, the target KMS Key is expected to have at least the following
 * KeyPolicy:
 * -------------
 * CustomCmkForQLDBExportEncryption:
 * Type: AWS::KMS::Key
 * Properties:
 * KeyUsage: ENCRYPT_DECRYPT
 * KeyPolicy:
 * Version: "2012-10-17"
 * Id: key-default-1
 * Statement:
 * - Sid: Grant Permissions for QLDB to use the key
 * Effect: Allow
 * Principal:
 * Service: us-east-1.qldb.amazonaws.com
 * Action:
 * - kms:Encrypt
 * - kms:GenerateDataKey
 * # In a key policy, you use "*" for the resource, which means "this CMK."
 * # A key policy applies only to the CMK it is attached to.
 * Resource: '*'
 * -------------
 * Please see the KMS key policy developer guide here:
 * https://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html
 */
object ExportJournal {
    val log = LoggerFactory.getLogger(ExportJournal::class.java)
    val client: AmazonQLDB = AmazonQLDBClientBuilder.standard().build()

    @JvmField
    val DEFAULT_EXPORT_TIMEOUT_MS = Duration.ofMinutes(10).toMillis()
    private const val POLICY_TEMPLATE = "{" +
            "   \"Version\" : \"2012-10-17\"," +
            "   \"Statement\": [ {statements} ]" +
            "}"
    var ASSUME_ROLE_POLICY = POLICY_TEMPLATE.replace(
        "{statements}",
        "   {" +
                "       \"Effect\": \"Allow\"," +
                "       \"Principal\": {" +
                "           \"Service\": [\"qldb.amazonaws.com\"]" +
                "       }," +
                "   \"Action\": [ \"sts:AssumeRole\" ]" +
                "   }"
    )
    private const val EXPORT_ROLE_S3_STATEMENT_TEMPLATE = "{" +
            "   \"Sid\": \"QLDBJournalExportS3Permission\"," +
            "   \"Action\": [\"s3:PutObject\", \"s3:PutObjectAcl\"]," +
            "   \"Effect\": \"Allow\"," +
            "   \"Resource\": \"arn:aws:s3:::{bucket_name}/*\"" +
            "}"
    private const val EXPORT_ROLE_KMS_STATEMENT_TEMPLATE = "{" +
            "   \"Sid\": \"QLDBJournalExportKMSPermission\"," +
            "   \"Action\": [\"kms:GenerateDataKey\"]," +
            "   \"Effect\": \"Allow\"," +
            "   \"Resource\": \"{kms_arn}\"" +
            "}"
    private const val EXPORT_ROLE_NAME = "QLDBTutorialJournalExportRole"
    private const val EXPORT_ROLE_POLICY_NAME = "QLDBTutorialJournalExportRolePolicy"
    private const val EXPORT_COMPLETION_POLL_PERIOD_MS = 10_000L
    private const val JOURNAL_EXPORT_TIME_WINDOW_MINUTES = 10L

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val s3BucketName: String
        var kmsArn: String? = null
        var roleArn: String? = null
        if (args.size >= 1) {
            s3BucketName = args[0]
            if (args.size >= 2) {
                roleArn = args[1]
            }
            // KMS Key ARN is an optional argument.
            // If not provided, SSE-S3 is used for exporting to S3 bucket.
            if (args.size == 3) {
                kmsArn = args[2]
            }
        } else {
            val accountId = AWSSecurityTokenServiceClientBuilder.defaultClient()
                .getCallerIdentity(GetCallerIdentityRequest()).account
            s3BucketName = Constants.JOURNAL_EXPORT_S3_BUCKET_NAME_PREFIX + "-" + accountId
        }
        val s3EncryptionConfiguration: S3EncryptionConfiguration
        s3EncryptionConfiguration = if (kmsArn == null) {
            S3EncryptionConfiguration().withObjectEncryptionType(S3ObjectEncryptionType.SSE_S3)
        } else {
            S3EncryptionConfiguration().withObjectEncryptionType(S3ObjectEncryptionType.SSE_KMS)
                .withKmsKeyArn(kmsArn)
        }
        createJournalExportAndAwaitCompletion(
            Constants.LEDGER_NAME, s3BucketName, Constants.LEDGER_NAME + "/",
            roleArn, s3EncryptionConfiguration, DEFAULT_EXPORT_TIMEOUT_MS
        )
    }

    /**
     * Create a new journal export on a S3 bucket and wait for its completion.
     *
     * @param ledgerName
     * The name of the bucket to be created.
     * @param s3BucketName
     * The name of the S3 bucket to create journal export on.
     * @param s3Prefix
     * The optional prefix name for the output objects of the export.
     * @param roleArn
     * The IAM role ARN to be used when exporting the journal.
     * @param encryptionConfiguration
     * The encryption settings to be used by the export job to write data in the given S3 bucket.
     * @param awaitTimeoutMs
     * Milliseconds to wait for export to complete.
     * @return [ExportJournalToS3Result] from QLDB.
     * @throws InterruptedException if thread is being interrupted while waiting for the export to complete.
     */
    @JvmStatic
    @Throws(InterruptedException::class)
    fun createJournalExportAndAwaitCompletion(
        ledgerName: String, s3BucketName: String,
        s3Prefix: String, roleArn: String?,
        encryptionConfiguration: S3EncryptionConfiguration,
        awaitTimeoutMs: Long
    ): ExportJournalToS3Result {
        val s3Client = AmazonS3ClientBuilder.defaultClient()
        createS3BucketIfNotExists(s3BucketName, s3Client)
        val roleArnNotNull = roleArn ?: createExportRole(
            EXPORT_ROLE_NAME, AmazonIdentityManagementClientBuilder.defaultClient(),
            s3BucketName, encryptionConfiguration.kmsKeyArn, EXPORT_ROLE_POLICY_NAME
        )

        return try {
            val startTime = Date.from(Instant.now().minus(JOURNAL_EXPORT_TIME_WINDOW_MINUTES, ChronoUnit.MINUTES))
            val endTime = Date.from(Instant.now())
            val exportJournalToS3Result = createExport(
                ledgerName, startTime, endTime, s3BucketName,
                s3Prefix, encryptionConfiguration, roleArnNotNull
            )

            // Wait for export to complete.
            waitForExportToComplete(Constants.LEDGER_NAME, exportJournalToS3Result.exportId, awaitTimeoutMs)
            log.info("JournalS3Export for exportId " + exportJournalToS3Result.exportId + " is completed.")
            exportJournalToS3Result
        } catch (e: Exception) {
            log.error("Unable to create an export!", e)
            throw e
        }
    }

    /**
     * If the bucket passed does not exist, the bucket will be created.
     *
     * @param s3BucketName
     * The name of the bucket to be created.
     * @param s3Client
     * The low-level S3 client.
     */
    fun createS3BucketIfNotExists(s3BucketName: String, s3Client: AmazonS3) {
        if (!s3Client.doesBucketExistV2(s3BucketName)) {
            log.info("S3 bucket $s3BucketName does not exist. Creating it now.")
            try {
                s3Client.createBucket(s3BucketName)
                log.info("Bucket with name $s3BucketName created.")
            } catch (e: Exception) {
                log.error("Unable to create S3 bucket named $s3BucketName")
                throw e
            }
        }
    }

    /**
     * Create a new export rule and a new managed policy for the current AWS account.
     *
     * @param roleName
     * Name of the role to be created.
     * @param iamClient
     * A low-level service client.
     * @param s3Bucket
     * If `kmsArn` is `null`, create a new ARN using the given bucket name.
     * @param kmsArn
     * Optional KMS Key ARN used to configure the `rolePolicyStatement`.
     * @param rolePolicyName
     * Name for the role policy to be created.
     * @return the newly created `roleArn`.
     */
    fun createExportRole(
        roleName: String, iamClient: AmazonIdentityManagement,
        s3Bucket: String, kmsArn: String, rolePolicyName: String
    ): String {
        val getRoleRequest = GetRoleRequest().withRoleName(roleName)
        return try {
            log.info("Trying to retrieve role with name: $roleName")
            val roleArn = iamClient.getRole(getRoleRequest).role.arn
            log.info("The role called $roleName already exists.")
            roleArn
        } catch (e: NoSuchEntityException) {
            log.info("The role called $roleName does not exist. Creating it now.")
            val createRoleRequest = CreateRoleRequest()
                .withRoleName(roleName)
                .withAssumeRolePolicyDocument(ASSUME_ROLE_POLICY)
            val roleArn = iamClient.createRole(createRoleRequest).role.arn
            val s3Statement = EXPORT_ROLE_S3_STATEMENT_TEMPLATE.replace("{bucket_name}", s3Bucket)
            val rolePolicyStatement =
                s3Statement + "," + EXPORT_ROLE_KMS_STATEMENT_TEMPLATE.replace("{kms_arn}", kmsArn)
            val rolePolicy = POLICY_TEMPLATE.replace("{statements}", rolePolicyStatement)
            val createPolicyResult = iamClient.createPolicy(
                CreatePolicyRequest()
                    .withPolicyName(rolePolicyName)
                    .withPolicyDocument(rolePolicy)
            )
            iamClient.attachRolePolicy(
                AttachRolePolicyRequest()
                    .withRoleName(roleName)
                    .withPolicyArn(createPolicyResult.policy.arn)
            )
            log.info("Role $roleName created with ARN: $roleArn and policy: $rolePolicy")
            roleArn
        }
    }

    /**
     * Request QLDB to export the contents of the journal for the given time
     * period and s3 configuration. Before calling this function the S3 bucket
     * should be created, see [.createS3BucketIfNotExists].
     *
     * @param name
     * Name of the ledger.
     * @param startTime
     * Time from when the journal contents should be exported.
     * @param endTime
     * Time until which the journal contents should be exported.
     * @param bucket
     * S3 bucket to write the data to.
     * @param prefix
     * S3 prefix to be prefixed to the files written.
     * @param s3EncryptionConfiguration
     * Encryption configuration for S3.
     * @param roleArn
     * The IAM role ARN to be used when exporting the journal.
     * @return [ExportJournalToS3Result] from QLDB.
     */
    fun createExport(
        name: String, startTime: Date, endTime: Date, bucket: String,
        prefix: String, s3EncryptionConfiguration: S3EncryptionConfiguration,
        roleArn: String
    ): ExportJournalToS3Result {
        log.info("Let's create a journal export for ledger with name: {}...", name)
        val s3ExportConfiguration = S3ExportConfiguration().withBucket(bucket).withPrefix(prefix)
            .withEncryptionConfiguration(s3EncryptionConfiguration)
        val request = ExportJournalToS3Request()
            .withName(name)
            .withInclusiveStartTime(startTime)
            .withExclusiveEndTime(endTime)
            .withS3ExportConfiguration(s3ExportConfiguration)
            .withRoleArn(roleArn)
        return try {
            val result = client.exportJournalToS3(request)
            log.info("Requested QLDB to export contents of the journal.")
            result
        } catch (ipe: InvalidParameterException) {
            log.error(
                "The eventually consistent behavior of the IAM service may cause this export"
                        + " to fail its first attempts, please retry."
            )
            throw ipe
        }
    }

    /**
     * Wait for the JournalS3Export to complete.
     *
     * @param ledgerName
     * Name of the ledger.
     * @param exportId
     * Optional KMS ARN used for S3-KMS encryption.
     * @param awaitTimeoutMs
     * Milliseconds to wait for export to complete.
     * @return [DescribeJournalS3ExportResult] from QLDB.
     * @throws InterruptedException if thread is interrupted while busy waiting for JournalS3Export to complete.
     */
    @Throws(InterruptedException::class)
    fun waitForExportToComplete(
        ledgerName: String,
        exportId: String,
        awaitTimeoutMs: Long
    ): DescribeJournalS3ExportResult {
        log.info("Waiting for JournalS3Export for " + exportId + "to complete.")
        var count = 0
        val maxRetryCount = awaitTimeoutMs / EXPORT_COMPLETION_POLL_PERIOD_MS + 1
        while (count < maxRetryCount) {
            val result = describeExport(ledgerName, exportId)
            if (result.exportDescription.status.equals(ExportStatus.COMPLETED.name, ignoreCase = true)) {
                log.info("JournalS3Export completed.")
                return result
            }
            log.info("JournalS3Export is still in progress. Please wait. ")
            Thread.sleep(EXPORT_COMPLETION_POLL_PERIOD_MS)
            count++
        }
        throw IllegalStateException("Journal Export did not complete for $exportId")
    }
}
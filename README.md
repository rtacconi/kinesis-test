https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-ddb.html
1. leaseKey string - the shardID - Primary partition key
2. checkpoint string - the latest processed sequence number
3. leaseCounter integer - Used for lease versioning so that workers can
detect that their lease has been taken by another worker.
4. leaseOwner string (uuid)

CREATE EXTERNAL FILE FORMAT [parquetfile1]
    WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = N'org.apache.hadoop.io.compress.SnappyCodec'
    );

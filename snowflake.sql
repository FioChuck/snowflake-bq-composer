CREATE STORAGE INTEGRATION gcs_integration TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'GCS' ENABLED = TRUE STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-landing/faa/');
DESC STORAGE INTEGRATION gcs_integration;
COPY INTO 'gcs://snowflake-landing/faa/'
FROM (
        SELECT DISTINCT object_id,
            global_id,
            faa_identifier,
            name,
            icao_id,
            airport_type,
            service_city,
            state_abbreviation,
            country
        FROM US_AIRPORTS
    ) OVERWRITE = TRUE FILE_FORMAT = (TYPE = csv COMPRESSION = NONE) STORAGE_INTEGRATION = gcs_integration;
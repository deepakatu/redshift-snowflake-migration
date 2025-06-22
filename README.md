# redshift-snowflake-migration

A modular ETL and data migration pipeline for moving data between Redshift, AWS S3, and Snowflake (or other data warehouse).  
All connection values must be supplied via environment variables or `.env` file for security.

## Features
- Connect to Redshift, S3, and Snowflake (or any compatible data warehouse)
- Modular ETL functions for batch migration, deduplication, and data hashing
- Data and secrets never hardcoded

## Usage

1. Copy `.env.example` to `.env` and fill in your secrets.
2. Install requirements:
pip install -r requirements.txt
3. Run the migration scripts as required:
python redshift_to_snowflake.py
python s3_to_snowflake.py
## Security
- **Never** store real credentials in code or public repos.
- Always use environment variables or secret managers.

## Author
*Anonymized for public sharing*
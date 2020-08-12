# ActionKit Userfield Sync

This is a small Python 3 script that loads data from a PostgreSQL database and syncs columns to ActionKit userfields. The query, columns, userfield names, and how many rows sync at a time are all configurable, so it can be used for a variety of needs. The provided partisanship.sql is an example.

You can also use this to sync multi-value fields by returning a comma-separated list in the query and including the column in the optional `SQL_LIST_COLUMNS` argument.

You can run this locally from a command line with only the requirements in `requirements.txt`. If you want to run tests and/or deploy the script to Lambda, you'll need the additional requirements in `dev_requirements.txt`.

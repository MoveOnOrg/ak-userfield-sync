from actionkit.api.user import AKUserAPI
import psycopg2
import psycopg2.extras
from pywell.entry_points import run_from_cli, run_from_lamba

DESCRIPTION = 'Sync values from query to ActionKit user fields.'

ARG_DEFINITIONS = {
    'AK_BASEURL': 'ActionKit Base URL.',
    'AK_USER': 'ActionKit API username.',
    'AK_PASSWORD': 'ActionKit API password.',
    'DB_HOST': 'Database host IP or hostname.',
    'DB_PORT': 'Database port number.',
    'DB_USER': 'Database user.',
    'DB_PASS': 'Database password.',
    'DB_NAME': 'Database name.',
    'SQL_FILE': 'Path to SQL file.',
    'SQL_LIMIT': 'How many records to process each run.',
    'SQL_COLUMNS': 'Comma-separated list off column names to sync.',
    'SQL_LIST_COLUMNS': 'Comma-separted list of column names that have values that are also comma-separated lists.',
    'AK_USERFIELDS': 'Comma-separted list of usefield names, in same order as mapped SQL_COLUMNS.'
}

REQUIRED_ARGS = [
    'AK_BASEURL', 'AK_USER', 'AK_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_USER',
    'DB_PASS', 'DB_NAME', 'SQL_FILE', 'SQL_LIMIT', 'SQL_COLUMNS', 'AK_USERFIELDS'
]

def get_rows(args):
    database = psycopg2.connect(
        host=args.DB_HOST,
        port=args.DB_PORT,
        user=args.DB_USER,
        password=args.DB_PASS,
        database=args.DB_NAME
    )
    database_cursor = database.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    with open(args.SQL_FILE, 'r') as file:
        query = file.read()
    query += ' LIMIT %d' % int(args.SQL_LIMIT)
    database_cursor.execute(query)
    return [dict(row) for row in database_cursor.fetchall()]


def sync(args):
    ak_user_api = AKUserAPI(args)
    rows = get_rows(args)
    columns = args.SQL_COLUMNS.split(',')
    if 'SQL_LIST_COLUMNS' in args.__dict__:
        list_columns = args.SQL_LIST_COLUMNS.split(',')
    else:
        list_columns = []
    userfields = args.AK_USERFIELDS.split(',')
    results = []
    for row in rows:
        key_values = {}
        for index, key in enumerate(userfields):
            column = columns[index]
            key_values[key] = row.get(column, '').encode('latin-1', 'ignore').decode('latin-1')
            if column in list_columns:
                key_values[key] = key_values[key].split(',')
        results.append((row.get('user_id'), key_values))
        ak_user_api.set_usertag(row.get('user_id'), key_values)
    return results


def aws_lambda(event, context) -> str:
     return run_from_lamba(
         sync, DESCRIPTION, ARG_DEFINITIONS, REQUIRED_ARGS, event
     )


if __name__ == '__main__':
    run_from_cli(sync, DESCRIPTION, ARG_DEFINITIONS, REQUIRED_ARGS)

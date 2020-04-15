# helper module with functions that assist with connecting Python and Postgres
import psycopg2


def parse_props(props_path):
    """Parses information needed to connect to db.
    Args:
        props_path: path to Java properties file.

    Returns: dictionary with parsed properties.
    """
    print("Parsing properties...")
    props = open(props_path).read().split("\n")
    delim = "="
    props = {kv.split(delim)[0]: kv.split(delim)[1] for kv in props if len(kv) >= 3}

    return props


def connect_db(props_path):
    """Connects to db and returns psycopg2 cursor object.
    Args:
        props_path: path to Java properties file.

    Returns:
        psycopg2 cursor object
    """
    props = parse_props(props_path)
    print("Connecting to db...")
    conn = psycopg2.connect(
        host=props['db_host'],
        database=props['db_name'],
        port=props['db_port'],
        user=props['db_user'],
        password=props['db_password']
    )
    print("\t--Connection established!")

    return conn.cursor()


def get_engine_info(props_path):
    """Helps with creating sqlalchemy engines."""
    props = parse_props(props_path)

    engine_info = "{flavor}://{user}:{password}@{host}:{port}/{database}".format(
        flavor='postgresql',
        user=props['db_user'],
        password=props['db_password'],
        host=props['db_host'],
        port=props['db_port'],
        database=props['db_name']
    )

    return engine_info


def main():
    pass


if __name__ == '__main__':
    main()

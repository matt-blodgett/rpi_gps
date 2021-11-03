import os
import sqlite3


PATH_DATABASE = 'gps.db'


def connect(path=PATH_DATABASE):
    return sqlite3.connect(path)


def exists(path=PATH_DATABASE):
    return os.path.exists(path)


def is_valid(path=PATH_DATABASE):
    try:
        connection = sqlite3.connect(path)
        cursor = connection.cursor()
        cursor.execute('SELECT COUNT(*) FROM session;')
    except sqlite3.DatabaseError:
        return False
    return True


def create(path=PATH_DATABASE):
    if os.path.exists(path):
        os.remove(path)

    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS sessions (
        session_id INTEGER PRIMARY KEY,
        timestamp_start TEXT,
        timestamp_end TEXT
    );
    """
    cursor.execute(sql)
    connection.commit()

    sql = """
    CREATE TABLE IF NOT EXISTS events (
        session_id INTEGER,
        timestamp TEXT NOT NULL,
        event TEXT NOT NULL,

        FOREIGN KEY (session_id) 
            REFERENCES session (session_id) 
                ON DELETE CASCADE 
                ON UPDATE NO ACTION
    );
    """
    cursor.execute(sql)
    connection.commit()

    sql = """
    CREATE TABLE IF NOT EXISTS logs (
        session_id INTEGER,
        timestamp TEXT,
        code TEXT,
        sentence TEXT,
        latitude REAL,
        longitude REAL,
        altitude REAL,
        speed REAL,
        fix_quality INTEGER,
        satellites INTEGER,
        track_angle REAL,
        horizontal_dilution REAL,
        height_geoid REAL,

        FOREIGN KEY (session_id) 
            REFERENCES session (session_id) 
                ON DELETE CASCADE 
                ON UPDATE NO ACTION
    );
    """
    cursor.execute(sql)
    connection.commit()

    connection.close()

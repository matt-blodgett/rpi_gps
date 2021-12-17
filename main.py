import sys
import time
import datetime
import threading
import logging
import argparse

import serial
import adafruit_gps

import database


def _help():
    print("""\
usage: main.py [-h] [-v | -q | -s]
               [--database-file DATABASE_FILE] [--overwrite-file] 
               [--max-records MAX_RECORDS] [--max-runtime MAX_RUNTIME]

optional arguments:
  -h | --help            show this help message and exit
  -v | --verbose         log all messages and increase verbosity
  -q | --quiet           suppress non-error logging messages
  -s | --silent          suppress all logging messages

database arguments:
  --database-file       specify a database file to either load or create
  --overwrite-file      overwrite database files that potentially exist

other arguments:
  --max-runtime         maximum program runtime in seconds before exiting
  --max-records         maximum number of records to fetch from device before exiting
""")


class GPSDataProcessor:

    def __init__(self):
        self._verbose = False

        self._database_file = None
        self._overwrite_file = False
        self._database_connection = None
        self._database_cursor = None
        self._database_session_id = None

        self._max_records = None
        self._max_runtime = None
        self._total_elapsed_runtime = 0
        self._total_processed_records = 0

        self._uart = None

        self._logger = logging.getLogger('rpi-gps')

    @staticmethod
    def _format_timestamp(ts):
        if ts:
            return f'{ts.tm_year}-{ts.tm_mon:02}-{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}'
        return None

    def _exit(self, status):
        self._process_event('SESSION_END')
        log = self._logger.warning if status == 0 else self._logger.fatal
        log('process: message="exited with status %s"', status)
        exit(status)

    def _check_processing_complete(self):
        if self._max_runtime is not None and self._total_elapsed_runtime >= self._max_runtime:
            self._logger.warning('process: message="max runtime limit reached at %s"', self._max_runtime)
            self._exit(0)
        elif self._max_records is not None and self._total_processed_records >= self._max_records:
            self._logger.warning('process: message="max records limit reached at %s"', self._max_records)
            self._exit(0)

    def _increment_elapsed_runtime(self, seconds=5):
        time.sleep(seconds)
        self._total_elapsed_runtime += seconds
        self._increment_elapsed_runtime(seconds)

    def _initialize_logger(self, args):
        level = logging.DEBUG

        if args.verbose:
            level = logging.DEBUG

        if args.quiet:
            level = logging.ERROR

        self._logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter_string = '%(asctime)s | %(levelname)s | %(message)s'
        formatter = logging.Formatter(formatter_string)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        if args.silent:
            self._logger.disabled = True

    def _initialize_settings(self, args):
        self._logger.debug('settings: message="initializing"')

        self._verbose = args.verbose
        self._max_records = args.max_records
        self._max_runtime = args.max_runtime

        self._logger.debug('settings: max_records="%s"', self._max_records)
        self._logger.debug('settings: max_runtime="%s"', self._max_runtime)

    def _initialize_database(self, args):
        self._logger.debug('database: message="initializing"')

        self._database_file = args.database_file
        self._overwrite_file = args.overwrite_file

        self._logger.debug('database: database_file="%s"', self._database_file)
        self._logger.debug('database: overwrite_file="%s"', self._overwrite_file)

        self._database_file = self._database_file or database.PATH_DATABASE

        if self._database_file == database.PATH_DATABASE:
            self._logger.debug('database: message="using default sqlite3 database file"')
        else:
            self._logger.debug('database: message="using supplied sqlite3 database file"')

        if self._overwrite_file or not database.exists(self._database_file):
            self._logger.debug('database: message="creating new sqlite3 database file"')
            database.create(self._database_file)
            self._database_connection = database.connect(self._database_file)
        elif database.exists(self._database_file):
            self._logger.debug('database: message="opening existing sqlite3 database file"')
            if database.is_valid(self._database_file):
                self._database_connection = database.connect(self._database_file)
            else:
                self._logger.error('database: error="invalid sqlite3 database file at "%s""', self._database_file)
                self._exit(-1)

        self._database_cursor = self._database_connection.cursor()
        self._logger.debug('database: message="created sqlite3 database connection successfully"')

        sql = """
        SELECT MAX(rowid) FROM sessions;
        """
        cursor.execute(sql)
        last_session_id = cursor.fetchone()[0] or 0
        session_id = last_session_id + 1
        self._database_session_id = session_id

        sql = """
        INSERT INTO sessions (session_id, timestamp_start, timestamp_end)
        VALUES (?, ?, ?);
        """
        parameters = [
            self._database_session_id,
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            None
        ]
        self._database_cursor.execute(sql, parameters)
        self._database_connection.commit()

        self._logger.debug('database: message="created new session id %s"', self._database_session_id)

    def _initialize_uart(self, args):
        serial_port = '/dev/ttyUSB0'
        serial_baudrate = 9600
        serial_timeout = 3000

        self._logger.debug('serial: message="initializing"')
        self._logger.debug('serial: port="%s"', serial_port)
        self._logger.debug('serial: baudrate="%s"', serial_baudrate)
        self._logger.debug('serial: timeout="%s"', serial_timeout)

        try:
            self.uart = serial.Serial(serial_port, baudrate=serial_baudrate, timeout=serial_timeout)
        except serial.serialutil.SerialException as err:
            self._logger.error('serial: error="%s"', err.strerror)
            self._exit(-1)

    def _initialize(self, args):
        self._initialize_logger(args)
        self._logger.debug('process: message="initializing"')
        self._initialize_settings(args)
        self._initialize_database(args)
        self._initialize_uart(args)

        if self._max_runtime is not None:
            thread = threading.Thread(target=self._increment_elapsed_runtime, daemon=True)
            thread.start()

    def _process_event(self, event):
        if event == 'FIX_WAIT':
            self._logger.debug('process: event="%s"', event)
        else:
            self._logger.info('process: event="%s"', event)

        sql = """
        INSERT INTO events (session_id, timestamp, event)
        VALUES (?, ?, ?);
        """
        parameters = [
            self._database_session_id,
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            event
        ]
        self._database_cursor.execute(sql, parameters)
        self._database_connection.commit()

        if event == 'SESSION_END':
            sql = """
            UPDATE sessions
            SET timestamp_end = ?
            WHERE session_id = ?;
            """
            parameters = [
                 datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                self._database_session_id
            ]
            self._database_cursor.execute(sql, parameters)
            self._database_connection.commit()

    def _process_data(self, gps):
        sql = """
        INSERT INTO logs (session_id, timestamp, code, sentence, latitude, longitude, altitude, speed, fix_quality, satellites, track_angle, horizontal_dilution, height_geoid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        code = None if not gps.nmea_sentence else gps.nmea_sentence[:gps.nmea_sentence.find(',')]
        parameters = [
            self._database_session_id,
            self._format_timestamp(gps.timestamp_utc),
            code,
            gps.nmea_sentence,
            gps.latitude,
            gps.longitude,
            gps.altitude_m,
            gps.speed_knots,
            gps.fix_quality,
            gps.satellites,
            gps.track_angle_deg,
            gps.horizontal_dilution,
            gps.height_geoid
        ]
        self._database_cursor.execute(sql, parameters)
        self._database_connection.commit()

        self._total_processed_records += 1

        if self._verbose:
             self._logger.debug('database: record="%s"', self._total_processed_records)

    def _mainloop(self):
        self._logger.debug('process: message="entering main processing loop"')

        self._process_event('SESSION_START')

        gps = adafruit_gps.GPS(self._uart)
        gps.send_command(b'PMTK314,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0')
        gps.send_command(b'PMTK220,1000')
        gps.send_command(b'PMTK605')

        fix_lost = False
        fix_wait_count = 0
        time_check = time.monotonic()

        while True:
            gps.update()

            time_current = time.monotonic()
            if time_current - time_check >= 1.0:
                time_check = time_current

                if not gps.has_fix:
                    if not fix_lost:
                        self._process_event('FIX_LOST')
                        fix_lost = True
                    else:
                        fix_wait_count += 1
                        if fix_wait_count > 5:
                            self._process_event('FIX_WAIT')
                            fix_wait_count = 0

                    self._check_processing_complete()

                else:
                    if fix_lost:
                        self._process_event('FIX_FOUND')
                        fix_lost = False
                        fix_wait_count = 0

                    self._process_data(gps)
                    self._check_processing_complete()

    def start(self, args):
        try:
            self._initialize(args)
            self._mainloop()
        except KeyboardInterrupt:
            self._exit(-1)


def main():
    if '-h' in sys.argv or '--help' in sys.argv:
        _help()
        exit(0)

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='store_true')
    group.add_argument('-q', '--quiet', action='store_true')
    group.add_argument('-s', '--silent', action='store_true')
    group = parser.add_argument_group('database arguments')
    group.add_argument('--database-file', type=str, default=None)
    group.add_argument('--overwrite-file', action='store_true')
    group = parser.add_argument_group('other arguments')
    group.add_argument('--max-runtime', type=int, default=None)
    group.add_argument('--max-records', type=int, default=None)
    args = parser.parse_args()

    gps_data_processor = GPSDataProcessor()
    gps_data_processor.start(args)


if __name__ == '__main__':
    main()

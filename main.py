import sys
import time
import datetime
import threading
import logging
import argparse

import serial
import adafruit_gps

import database


# GPS quality indicator
# 0 - fix not available,
# 1 - GPS fix,
# 2 - Differential GPS fix (values above 2 are 2.3 features)
# 3 - PPS fix
# 4 - Real Time Kinematic
# 5 - Float RTK
# 6 - estimated (dead reckoning)
# 7 - Manual input mode
# 8 - Simulation mode


# self._output = 'parsed'
# ========================================
# $GPRMC,HHMMSS.000,A,XXXX.XXXX,N,0YYYY.YYYY,W,1.95,120.35,DDMMYY,,,D*7E
# timestamp = YYYY-MM-DD HH:MM:SS
# latitude: XX.XXXXX degrees
# longitude: -YY.YYYYYY degrees
# fix quality: 2
# fix quality 3d: 0
# # satellites: 4
# altitude: 198.5 meters
# speed: 1.95 knots
# track angle: 120.35 degrees
# horizontal dilution: 9.14
# height geoid: -35.0 meters
# ========================================


def _help():
    print("""\
usage: main.py [-h] [-v | -q | -s] [-o {raw,parsed,database}]
               [--database-file DATABASE_FILE] [--overwrite-file] 
               [--max-records MAX_RECORDS] [--max-runtime MAX_RUNTIME]

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         allow all logging messages
  -q, --quiet           suppress non-error logging messages
  -s, --silent          suppress all logging messages

output arguments:
  -o, --output          processed data output handling {raw,parsed,database}

database arguments:
  --database-file       specify a database file to either load or create
  --overwrite-file      overwrite database files that potentially exist

other arguments:
  --max-runtime         maximum program runtime in seconds before exiting
  --max-records         maximum number of records to fetch from device before exiting
""")


class GPSDataProcessor:

    def __init__(self):
        self._output = None
        self._verbose = False
        self._max_records = None
        self._max_runtime = None

        self._database_file = None
        self._overwrite_file = False
        self._database_connection = None
        self._database_cursor = None
        self._database_session_id = None

        self._uart = None

        self._total_elapsed_runtime = 0
        self._total_processed_records = 0

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

        if args.silent or args.output != 'database':
            self._logger.disabled = True

    def _initialize_settings(self, args):
        self._logger.debug('settings: message="initializing"')

        self._output = args.output
        self._verbose = args.verbose
        self._max_records = args.max_records
        self._max_runtime = args.max_runtime

        self._logger.debug('settings: output="%s"', self._output)
        self._logger.debug('settings: max_records="%s"', self._max_records)
        self._logger.debug('settings: max_runtime="%s"', self._max_runtime)

    def _initialize_database(self, args):
        if self._output != 'database':
            return

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
            if self._output != 'database':
                print(f'serial error: "{err.strerror}"')
            self._logger.error('serial: error="%s"', err.strerror)
            self._exit(-1)

    def _initialize(self, args):
        self._initialize_logger(args)
        self._logger.debug('process: message="initializing"')
        self._initialize_settings(args)
        self._initialize_database(args)
        self._process_event('SESSION_INITIALIZE')
        self._initialize_uart(args)

        if self._max_runtime is not None:
            thread = threading.Thread(target=self._increment_elapsed_runtime, daemon=True)
            thread.start()

    def _output_data_raw(self, gps):
        print(gps.nmea_sentence)

    def _output_data_parsed(self, gps):
        print('=' * 40)
        print(f'{gps.nmea_sentence}')
        print(f'timestamp: {self._format_timestamp(gps.timestamp_utc)}')
        print(f'latitude: {gps.latitude:.6f} degrees')
        print(f'longitude: {gps.longitude:.6f} degrees')
        print(f'altitude: {gps.altitude_m:.02} meters')
        print(f'speed: {gps.speed_knots} knots')
        print(f'fix quality: {gps.fix_quality}')
        print(f'satellites: {gps.satellites}')
        print(f'track angle: {gps.track_angle_deg} degrees')
        print(f'horizontal dilution: {gps.horizontal_dilution}')
        print(f'height geoid: {gps.height_geoid} meters')

        if self._verbose:
            # print all gps properties?
            # attributes = [
            #     'timestamp_utc',
            #     'latitude',
            #     'longitude',
            #     'fix_quality',
            #     'fix_quality_3d',
            #     'satellites',
            #     'satellites_prev',
            #     'horizontal_dilution',
            #     'altitude_m',
            #     'height_geoid',
            #     'speed_knots',
            #     'track_angle_deg',
            #     'sats',
            #     'isactivedata',
            #     'true_track',
            #     'mag_track',
            #     'sat_prns',
            #     'sel_mode',
            #     'pdop',
            #     'hdop',
            #     'vdop',
            #     'total_mess_num',
            #     'mess_num',
            #     'debug'
            # ]
            # print(f'dst: {ts.tm_isdst}')
            # fix_quality_3d = 0
            # satellites_prev = None
            # sats = None
            # isactivedata = A
            # true_track = None
            # mag_track = None
            # sat_prns = None
            # sel_mode = None
            # pdop = None
            # hdop = None
            # vdop = None
            # total_mess_num = None
            # mess_num = None
            # debug = False
            pass

    def _output_data_database(self, gps):
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

        if self._verbose:
             self._logger.debug('database: record="%s"', self._total_processed_records)

    def _process_data(self, gps):
        if self._output == 'raw':
            self._output_data_raw(gps)

        elif self._output == 'parsed':
            self._output_data_parsed(gps)

        elif self._output == 'database':
            self._output_data_database(gps)

        self._total_processed_records += 1

    def _process_event(self, event):
        if event == 'FIX_WAIT':
            if self._output == 'raw':
                print('waiting for a fix')

            elif self._output == 'parsed':
                print('waiting for a fix (parsed)')

            elif self._output == 'database':
                self._logger.debug('process: message="waiting for a fix"')

            return

        if self._output == 'raw':
            print(event)

        elif self._output == 'parsed':
            print(event, '(parsed)')

        elif self._output == 'database':
            self._logger.info('process: event="%s"', event)

            sql = """
            INSERT INTO activity (session_id, timestamp, event)
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

    def _mainloop(self):
        self._logger.debug('process: message="entering main processing loop"')

        gps = adafruit_gps.GPS(self._uart)
        gps.send_command(b'PMTK314,0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0')
        gps.send_command(b'PMTK220,1000')
        gps.send_command(b'PMTK605')

        fix_lost = False
        fix_wait_count = 0
        time_check = time.monotonic()

        self._process_event('SESSION_START')

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
    group = parser.add_argument_group('output arguments')
    group.add_argument('-o', '--output', choices=['raw', 'parsed', 'database'], default='parsed')
    group = parser.add_argument_group('database arguments')
    group.add_argument('--database-file', type=str, default=None)
    group.add_argument('--overwrite-file', action='store_true')
    group = parser.add_argument_group('other arguments')
    group.add_argument('--max-runtime', type=int, default=None)
    group.add_argument('--max-records', type=int, default=None)
    args = parser.parse_args()

    if args.output in ['raw', 'parsed']:
        if args.database_file is not None or args.overwrite_file:
            parser.error('database flags are not used when the --output flag is not "database"')

    gps_data_processor = GPSDataProcessor()
    gps_data_processor.start(args)


if __name__ == '__main__':
    main()

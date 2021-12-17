import time

import serial
import adafruit_gps


def main():
    uart = None
    serial_port = '/dev/ttyUSB0'
    serial_baudrate = 9600
    serial_timeout = 3000

    try:
        uart = serial.Serial(serial_port, baudrate=serial_baudrate, timeout=serial_timeout)
    except serial.serialutil.SerialException as err:
        print(err.strerror)
        exit(-1)

    gps = adafruit_gps.GPS(uart)
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
                    print('FIX_LOST')
                    fix_lost = True
                else:
                    fix_wait_count += 1
                    if fix_wait_count > 5:
                        print('FIX_WAIT')
                        fix_wait_count = 0
            else:
                if fix_lost:
                    print('FIX_FOUND')
                    fix_lost = False
                    fix_wait_count = 0

                print(gps.nmea_sentence)


if __name__ == '__main__':
    main()


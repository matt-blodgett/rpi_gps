# rpi_gps

A utility built for Raspberry Pis to interface with the [Adafruit Ultimate GPS breakout board](https://learn.adafruit.com/adafruit-ultimate-gps)

The `main.py` script takes several arguments, most importantly `--output`

## Output Types

The `--output` flag values:

| output type  | description |
|-|-|
| raw | write **only** raw nmea sentences read from the device to stdout |
| parsed | write parsed and formatted information read from the device to stdout |
| database | write information read from the device to a sqlite3 database |

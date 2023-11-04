import argparse
import logging
import os
from typing import Optional
from db.weatherdb import WeatherFinder

"""
Export t_weather to CSV file.
for python 3.7.x
"""

# SQLite3 Databaseファイルパス
PATH_WEATHER_DB: str = os.environ.get("PATH_WEATHER_DB", "~/db/weather.db")
# CSV出力パス
OUTPUT_CSV_PATH = os.environ.get("OUTPUT_CSV_PATH", "~/Downloads/csv/")


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
    app_logger: logging.Logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)
    app_logger.info(f"PATH_WEATHER_DB: {PATH_WEATHER_DB}")
    app_logger.info(f"OUTPUT_CSV_PATH: {OUTPUT_CSV_PATH}")

    parser = argparse.ArgumentParser()
    parser.add_argument("--device-name", type=str, required=True,
                        help="Device name with t_device name.")
    parser.add_argument("--date-from", type=str, required=True,
                        help="Date from with t_weather.measurement_time.")
    parser.add_argument("--date-to", type=str, required=True,
                        help="Date to with t_weather.measurement_time.")
    args: argparse.Namespace = parser.parse_args()
    app_logger.info(args)

    weather_finder: Optional[WeatherFinder] = None
    db_path: str = os.path.expanduser(PATH_WEATHER_DB)
    try:
        weather_finder = WeatherFinder(db_path, logger=app_logger)
        app_logger.info(weather_finder)
        # from t_weather to csv
        csv_iterable = weather_finder.find(
            args.device_name, date_from=args.date_from, date_to=args.date_to
        )
        app_logger.info(f"type(csv_iterable): {type(csv_iterable)}")
        # filename: build "" + "device name" + "date_from" + "date_to" + "date now" + ".csv"
        csv_file: str = os.path.join(
            os.path.expanduser(OUTPUT_CSV_PATH), weather_finder.csv_filename
        )
        with open(csv_file, 'w', newline='') as fp:
            fp.write(WeatherFinder.CSV_WEATHER_HEADER)
            if csv_iterable is not None:
                for line in csv_iterable:
                    fp.write(line + "\n")
        app_logger.info("Saved Weather CSV: {}".format(csv_file))
    except Exception as e:
        app_logger.warning("WeatherFinder error: {}".format(e))
    finally:
        if weather_finder is not None:
            weather_finder.close()

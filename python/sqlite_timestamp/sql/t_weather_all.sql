SELECT
   measurement_time,
   datetime(measurement_time, 'unixepoch', 'localtime'), 
   temp_out, temp_in, humid, pressure
FROM 
   t_weather
WHERE
   did=(SELECT id FROM t_device WHERE name='esp8266_1') 
ORDER BY measurement_time;


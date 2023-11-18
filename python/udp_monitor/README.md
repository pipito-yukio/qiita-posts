[Qiita] 「ラズパイ Pythonアプリケーションをシステムサービス化する」で解説したソースコードです

```
├── 1_inst_pythonapp.sh
├── bin
│     ├── pigpio
│     │     ├── UdpMonitorFromWeatherSensor.py
│     │     ├── conf
│     │     │     └── logconf_service_weather.json
│     │     └── log
│     │         ├── __init__.py
│     │         └── logsetting.py
│     └── udp_monitor_from_weather_sensor.sh
├── logs
│     └── pigpio
└── work
    ├── etc
    │     ├── default
    │     │     └── udp-weather-mon
    │     └── systemd
    │         └── system
    │             └── udp-weather-mon.service
    └── requirements.txt
```


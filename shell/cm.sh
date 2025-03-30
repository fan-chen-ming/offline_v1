cd /opt/soft/datax/job/chenming
python3 gen_import_config.py -d xxx -t  xxx
sh mysql_to_hdfs all
 ./bin/seatunnel.sh --config /opt/soft/seatunnel-2.3.10/user_config/2207A/chenming/my.conf


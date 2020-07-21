import datetime
from mysql.connector import MySQLConnection, Error, connect
import pandas
from mySQL_conn import Connection


def store_pass_data(all_passes, table_name):
    print("all_passes: ", all_passes)

    mydb = connect_to_mysql_db()
    mycursor = mydb.cursor()
    # truncate table if not empty?
    mycursor.execute("truncate table {0};".format(table_name))
    update_date = datetime.datetime.utcnow()
    sql = "INSERT INTO {0} (city, risetime, duration_in_seconds, update_date) VALUES (%s, %s, %s, %s)".format(table_name)
    val_tuples = [(p['city'], convert_epoch_to_datetime(p['risetime']), p['duration'], update_date) for p in all_passes]
    mycursor.executemany(sql, val_tuples)

    mydb.commit()
    print(mycursor.rowcount, "was inserted.")


def connect_to_mysql_db():
    return connect(
        host=Connection.host,
        user=Connection.user,
        password=Connection.password,
        database=Connection.database
    )


def convert_epoch_to_datetime(epoch):
    return datetime.datetime.utcfromtimestamp(epoch)


def update_city_avg_daily_flights():
    try:
        stp_name = 'update_city_avg_daily_flights_achinoam'
        db = connect_to_mysql_db()
        cursor = db.cursor()
        cursor.callproc(stp_name, ())
        result = cursor.fetchone()
        print(result)
    except Error as e:
        print(e)


def combine_stats_data_and_export_scv(csv_file_path):
    mydb = connect_to_mysql_db()
    query = get_query_to_combine_all_stats()
    results = pandas.read_sql_query(query, mydb)
    results.to_csv(csv_file_path, index=False)


def get_query_to_combine_all_stats():
    return "select stats.*, achi.daily_avg, achi.update_date as achi_update_date " \
           "from city_stats_achinoam achi " \
           "join(" \
           "select * from city_stats_beer_sheva " \
           "union select * from city_stats_eilat " \
           "union select * from city_stats_haifa " \
           "union select * from city_stats_tel_aviv " \
           ") stats on achi.city = stats.city"

import boto3
import pandas as pd
import pymysql
from io import StringIO, BytesIO

db_user = ''
db_passwd = ''


table_list = [
''
]

db_info = {'db_name':'db_ip'}

#conn = pymysql.connect(host='', user=db_user, password=db_passwd,port='')
#curs = conn.cursor()
#sql = ""
#curs.execute(sql)
#result = curs.fetchall()
#curs.close()
#conn.close()

seq = 0
seq_file = 0
for i in range(len(db_info)):

    db_nm = db_info[i][0]
    db_ip = db_info[i][1]

    dbip_conn = pymysql.connect(host=db_ip, user=db_user, password=db_passwd, db=db_nm, charset='utf8')
    dbip_curs = dbip_conn.cursor()

    for j in range(len(table_list)):

        table_nm = table_list[j]

        min_max_id_sql = "SELECT MIN(id), MAX(id) FROM " + table_nm

        try:

            dbip_curs.execute(min_max_id_sql)
            min_max_id = dbip_curs.fetchall()
            min_id = min_max_id[0][0]
            max_id = min_max_id[0][1]

            seq = j + 1

            column_select = """
                                SELECT  column_name, column_type
                                FROM   information_schema.COLUMNS 
                                WHERE  table_schema = '%s'
                                AND    table_name = '%s'
            """

            dbip_curs.execute(column_select % (db_nm, table_nm))
            column_result = dbip_curs.fetchall()

            select_query = "SELECT '" + db_nm + "',"

            for c in range(len(column_result)):

                column_name = column_result[c][0]
                column_type = column_result[c][1]

                if column_type == 'datetime' or column_type == 'timestamp':
                    column_name = "CASE WHEN " + column_name + " = '0000-00-00 00:00:00' THEN '0101-01-01' ELSE " + column_name + " END AS '" + column_name + "'"

                if column_type == 'date':
                    column_name = "CASE WHEN " + column_name + " = '0000-00-00 00:00:00' THEN '0101-01-01' ELSE " + column_name + " END AS '" + column_name + "'"

                if c + 1 == len(column_result):

                    result_query = select_query + column_name + " FROM " + db_nm + "." + table_nm + " WHERE id BETWEEN "

                    if (max_id - min_id) <= 1500000:
                        print(min_id, max_id)
                        result_query = result_query + str(min_id) + " AND " + str(max_id)

                        df = pd.read_sql_query(result_query, dbip_conn)
                        csv_buffer = BytesIO()
                        df.to_csv(csv_buffer, date_format='%Y-%m-%d %H:%M:%S', sep=',', index=False, encoding='utf-8',header=False, chunksize=200000, compression='gzip')
                        s3_resource = boto3.resource('s3', aws_access_key_id='',aws_secret_access_key='')
                        s3_resource.Object('test-dwayne', db_nm + '/' + table_nm + '/' + str(seq_file + 1) + '.csv.gz').put(Body=csv_buffer.getvalue())

                    else:
                        seq_file += 1
                        next_id = min_id + 1500000

                        loop_result_query = result_query
                        loop_result_query = loop_result_query + str(min_id) + " AND " + str(next_id)
                        print(loop_result_query)
                        print("\n")
                        df = pd.read_sql_query(loop_result_query, dbip_conn)
                        csv_buffer = BytesIO()
                        df.to_csv(csv_buffer, date_format='%Y-%m-%d %H:%M:%S', sep=',', index=False, encoding='utf-8',header=False, chunksize=200000, compression='gzip')
                        s3_resource = boto3.resource('s3', aws_access_key_id='',aws_secret_access_key='')
                        s3_resource.Object('test-dwayne', db_nm + '/' + table_nm + '/' + str(seq_file) + '.csv.gz').put(Body=csv_buffer.getvalue())

                        next_start_id = next_id + 1

                        while next_start_id < max_id:
                            seq_file += 1
                            loop_result_query = result_query
                            next_end_id = next_start_id + 1500000

                            loop_result_query = loop_result_query + str(next_start_id) + " AND " + str(next_end_id)
                            print(loop_result_query)
                            print("\n")
                            df = pd.read_sql_query(loop_result_query, dbip_conn)
                            csv_buffer = BytesIO()
                            df.to_csv(csv_buffer, date_format='%Y-%m-%d %H:%M:%S', sep=',', index=False, encoding='utf-8',header=False, chunksize=200000, compression='gzip')
                            s3_resource = boto3.resource('s3', aws_access_key_id='',aws_secret_access_key='')
                            s3_resource.Object('test-dwayne',db_nm + '/' + table_nm + '/' + str(seq_file) + '.csv.gz').put(Body=csv_buffer.getvalue())

                            next_start_id = next_end_id + 1

                            if (max_id - next_start_id) < 1500000:
                                result_query = result_query + str(next_start_id) + " AND " + str(max_id)
                                df = pd.read_sql_query(result_query, dbip_conn)
                                csv_buffer = BytesIO()
                                df.to_csv(csv_buffer, date_format='%Y-%m-%d %H:%M:%S', sep=',', index=False, encoding='utf-8',header=False, chunksize=200000, compression='gzip')
                                s3_resource = boto3.resource('s3', aws_access_key_id='',aws_secret_access_key='')
                                s3_resource.Object('test-dwayne',db_nm + '/' + table_nm + '/' + str(seq_file + 1) + '.csv.gz').put(Body=csv_buffer.getvalue())
                                print(result_query)
                                print("\n")
                                break

                else:
                    select_query = select_query + column_name + ","

        except Exception as error:
            continue

    dbip_curs.close()
    dbip_conn.close()
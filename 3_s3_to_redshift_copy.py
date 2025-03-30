import psycopg2
import pymysql
import sys

db_user = ''
db_passwd = ''


table_list = [
'tx1',

]

db_info = {
    'db_name':'db_ip'
}


seq = 0
sys.stdout = open('/home/centos/python-script/test/upload_log','w')

for j in range(len(db_info)):

    db_nm = db_info[j][0]

    dw_conn = psycopg2.connect(host='',port='', user=db_user, password=db_passwd, dbname='')
    dw_curs = dw_conn.cursor()
    print(db_nm,'-------------------------------------------------------')
    sys.stdout.flush()

    seq = 0
    for i in range(len(table_list)):

        table_nm = table_list[i]
        csv_upload_sql = "COPY " + table_nm +" FROM 's3://test-dwayne/"+ db_nm +"/"+ table_nm +"/' ACCESS_KEY_ID '' SECRET_ACCESS_KEY '' CSV GZIP"

        try:
            
            dw_curs.execute(csv_upload_sql)
            dw_conn.commit()
            seq = i + 1
            print(seq,'. ',table_nm)
            sys.stdout.flush()

        except Exception as error:

            msg = str(error)
            if msg.find('does not exist'):
                continue

            print(error)
            sys.stdout.flush()
            continue

        finally:
            dw_conn.commit()

    dw_curs.close()
    dw_conn.close()

sys.stdout.close()
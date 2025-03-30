import pymysql
import psycopg2

i_table_nm = input('테이블명 입력 : ').split(',')


conn = pymysql.connect(host='', user='', password='', charset='utf8')
curs = conn.cursor()

for t in range(len(i_table_nm)):
    covert_table_text = ''
    table_nm = i_table_nm[t]
    create_query = """
          SELECT  column_name
             ,  CASE WHEN data_type = 'int' THEN 'int4'
                    WHEN data_type = 'tinyint' THEN 'int2'
                    WHEN data_type = 'datetime' THEN 'timestamp'
                    WHEN data_type = 'text' THEN 'varchar' ELSE data_type END AS 'data_type'
             ,  IF(character_octet_length IS NULL,'',character_octet_length) AS 'character_octet_length'
             ,  CASE WHEN is_nullable = 'NO' THEN 'NULL'
                    WHEN is_nullable = 'YES' THEN 'NULL' END AS 'is_nullable'
          FROM   information_schema.COLUMNS 
          WHERE  table_schema = '%s'
          AND    table_name = '%s'
    """
    covert_table_text = 'CREATE TABLE '+ table_nm + ' ( member_id VARCHAR(100) NOT NULL, '
    column_text = ''
    curs.execute(create_query % table_nm)
    result = curs.fetchall()

    for i in range(len(result)):

        column_nm = result[i][0]
        column_type = result[i][1]
        column_length = result[i][2]
        column_null = result[i][3]

        column_data_type = ''
        if result[i][2] != '':
            column_data_type = column_type + '(' + column_length + ')'
            pass
        else:
            column_data_type = column_type

        if i + 1 == len(result):
            column_text += column_nm + ' ' + column_data_type + ' ' + column_null
        else:
            column_text += column_nm + ' ' + column_data_type + ' ' + column_null + ','

    covert_table_text = covert_table_text + column_text + ');'
    print(covert_table_text)

    dw_conn = psycopg2.connect(host='', port='' , user='', password='', dbname='')
    dw_curs = dw_conn.cursor()
    dw_curs.execute(covert_table_text)
    dw_conn.commit()


    comment_query = """
          SELECT  column_name, column_comment
            FROM   information_schema.COLUMNS 
            WHERE  table_schema = '%s'
            AND    table_name = '%s'
    """
    covert_comment_text = 'COMMENT ON COLUMN '+ table_nm
    curs.execute(comment_query % table_nm)
    comment_result = curs.fetchall()

    for j in range(len(comment_result)):

        column_nm = comment_result[j][0]
        column_commnet = comment_result[j][1]

        comment_text = ''
        if column_commnet == '':
            continue
        else:
            comment_text = covert_comment_text + '.' + column_nm + ' IS \'' + column_commnet + '\';'
            dw_curs.execute(comment_text)
            dw_conn.commit()
            print(comment_text)

    print("\n")
    dw_curs.close()
    dw_conn.close()

curs.close()
conn.close()
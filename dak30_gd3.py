import pandas as pd
import psycopg2
from datetime import datetime
import matplotlib.pyplot as plt
import xlsxwriter
from pytrends.request import TrendReq


def connect():
    """ Connect to the PostgreSQL database server """
    conn, cur = None, None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost", port="5432",
            database="postgres",
            user="postgres",
            password="1122000")
        # create a cursor
        cur = conn.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while excuting SQL" + error)

    return conn, cur


def input_data():
    try:
        conn, cur = connect()
        cur.execute("""
        DROP TABLE IF EXISTS vn_trending;
        CREATE TABLE vn_trending (
                    Id INT GENERATED ALWAYS AS IDENTITY,
                    keyword VARCHAR(255) NOT NULL,
                    date DATE,
                    Value integer,
                    trend_type VARCHAR(255),
                    CONSTRAINT vn_trending_pkey PRIMARY KEY (id)
                )
        TABLESPACE pg_default;
        ALTER TABLE public.vn_trending
        OWNER to postgres;
        """)
        postgres_insert_query = """INSERT INTO vn_trending (keyword, date, Value, trend_type) VALUES (%s, %s, %s, %s)"""
        # close communication with the PostgresSQL database server
        # connect to gg trends
        pytrends = TrendReq()
        df = pd.read_excel('keytrends.xlsx')
        columns_name = list(df.columns)

        for col in columns_name:
            df_ = df[col]
            df_.dropna(inplace=True)
            keywords = df_.values.tolist()
            for kw in keywords:
                pytrends.build_payload(
                    kw_list=[kw],
                    cat=0,
                    timeframe='2019-01-01 2020-12-31',
                    geo='VN',
                    gprop=''
                )
                data = pytrends.interest_over_time()
                if not data.empty:
                    data = data.drop(labels=['isPartial'], axis='columns')
                index = data.index
                a_list = list(index)
                i = 0
                for cell in data[kw]:
                    record_to_insert = (kw, a_list[i], int(cell), col.replace('/', '_'))
                    cur.execute(postgres_insert_query, record_to_insert)
                    i = i + 1
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def write_report():
    conn, cur = connect()
    cur.execute("""
        SELECT DISTINCT trend_type
        FROM public.vn_trending
    """)
    db = pd.DataFrame(cur.fetchall())
    sheet_list = db[0].tolist()
    # create Excel file
    workbook = xlsxwriter.Workbook('vn_trending_search_keyword_2020.xlsx')
    # write Excel in seperate sheet
    for sheet in sheet_list:
        # Create sheet Excel
        worksheet = workbook.add_worksheet(name=sheet)
        # add format and title columns
        cellformat = workbook.add_format()
        cellformat.set_center_across()
        cellformat.set_bold()
        worksheet.merge_range('B1:M1', 'TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM', cellformat)
        worksheet.merge_range('B2:M2', 'Năm 2020', cellformat)
        worksheet.write('A3', 'STT', cellformat)
        worksheet.write('B3', 'Keyword', cellformat)
        worksheet.write('C3', 'Tháng 1', cellformat)
        worksheet.write('D3', 'Tháng 2', cellformat)
        worksheet.write('E3', 'Tháng 3', cellformat)
        worksheet.write('F3', 'Tháng 4', cellformat)
        worksheet.write('G3', 'Tháng 5', cellformat)
        worksheet.write('H3', 'Tháng 6', cellformat)
        worksheet.write('I3', 'Tháng 7', cellformat)
        worksheet.write('J3', 'Tháng 8', cellformat)
        worksheet.write('K3', 'Tháng 9', cellformat)
        worksheet.write('L3', 'Tháng 10', cellformat)
        worksheet.write('M3', 'Tháng 11', cellformat)
        worksheet.write('N3', 'Tháng 12', cellformat)
        cur.execute("""
                    SELECT DISTINCT keyword
                    FROM public.vn_trending
                    WHERE trend_type = %s
                    """, [sheet])
        db = pd.DataFrame(cur.fetchall())
        keyword_list = db[0].tolist()
        worksheet.set_column('B:B', 30)
        for row in range(3, len(keyword_list) + 3):
            worksheet.write(row, 0, row - 2, cellformat)
            worksheet.write(row, 1, keyword_list[row - 3])
            month = 1
            for col in range(2, 14):
                cur.execute("""
                    SELECT SUM(vn_trending.value)
                    FROM public.vn_trending
                    WHERE EXTRACT(YEAR FROM vn_trending.date) = 2020 
                      AND EXTRACT(MONTH FROM vn_trending.date) = %s
                      AND keyword = %s
                """, [month, keyword_list[row-3]])
                db = pd.DataFrame(cur.fetchall())
                result = int(db[0])
                worksheet.write(row, col, result)
                month += 1
        # # Add an Excel date format.
        # date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
    workbook.close()
    # writer = pd.ExcelWriter('vn_trending_result.xlsx', engine='xlsxwriter')
    cur.close()
    # commit the changes
    conn.commit()
    conn.close()
    # db.to_excel(writer, sheet_name='Top 10 key words')
    # # # close the Pandas Excel writer and output the Excel file


def query_top_2020():
    conn, cur = connect()
    cur.execute("""
            SELECT keyword, SUM(vn_trending.Value) AS totalValue
            from public.vn_trending
            WHERE EXTRACT(YEAR FROM vn_trending.date) = 2020 
            Group by vn_trending.keyword
            ORDER BY totalValue DESC
            LIMIT 5
            """)

    db = pd.DataFrame(cur.fetchall())
    db.rename(columns={0: 'keyword', 1: 'totalValue'}, inplace=True)
    db.index = pd.RangeIndex(start=1, stop=len(db) + 1, step=1)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(db['keyword'], db['totalValue'])
    plt.ylim(db['totalValue'][5] - 200, db['totalValue'][1] + 200)
    ax.set(title="TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM 2020")
    plt.savefig('top_search_key_2020.png')
    cur.close()
    # commit the changes
    conn.commit()
    conn.close()

def query_top_2019():
    conn, cur = connect()
    cur.execute("""
            SELECT keyword, SUM(vn_trending.Value) AS totalValue
            from public.vn_trending
            WHERE EXTRACT(YEAR FROM vn_trending.date) = 2019 
            Group by vn_trending.keyword
            ORDER BY totalValue DESC
            FETCH  FIRST  5 rows only
        """)

    db = pd.DataFrame(cur.fetchall())
    db.rename(columns={0: 'keyword', 1: 'totalValue'}, inplace=True)
    db.index = pd.RangeIndex(start=1, stop=len(db) + 1, step=1)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(db['keyword'], db['totalValue'])
    plt.ylim(db['totalValue'][5] - 200, db['totalValue'][1] + 200)
    ax.set(title="TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM 2019")
    plt.savefig('top_search_key_2019.png')
    cur.close()
    # commit the changes
    conn.commit()
    conn.close()


if __name__ == '__main__':
    input_data()
    write_report()
    query_top_2019()
    query_top_2020()

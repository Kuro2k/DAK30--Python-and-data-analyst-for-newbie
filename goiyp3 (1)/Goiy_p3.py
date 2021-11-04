import pandas as pd
import psycopg2


s3 = '3. Xuất báo cáo search keyword in 2020'
s4 = '4. Vẽ biểu đồ line chart top 5 trending các từ khóa tìm kiếm nhiều nhất 2020'
s5 = '5. Vẽ biểu đồ bar chart top 5 trending các từ khóa tìm kiếm nhiều nhất 2019'

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


def search_key_word():
    print(s3)
    sql = """SELECT trend_type,keyword, to_char(date::date,'mm/yyyy') monthly, sum(VALUE) sum_val
                FROM vn_trending
                WHERE EXTRACT(YEAR FROM date::DATE) = 2020
                GROUP BY trend_type,keyword,to_char(date::date,'mm/yyyy') 
                ORDER BY trend_type, keyword;
            """
    conn, cur = connect()
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd, columns=['trend_type', 'keyword', 'monthly', 'sum_val'])
    df2 = df.pivot_table(index="keyword", columns="monthly", values='sum_val')
    writer = pd.ExcelWriter('vn_trending_search_keyword_2010.xlsx')
    df2.to_excel(writer)
    writer.save()

def top_five_trending(year='2019, 2020'):
        conn, cur = connect()
        if year == '2020':
            print(s4)
            sql = build_sql(year, 5)
            cur.execute(sql)
            rd = cur.fetchall()
            df = pd.DataFrame(rd, columns=['stt', 'keyword', 'sum_val', 'monthly', 'max_val'])
            if len(df):
                image = df.plot(title='TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM 2020')
                fig = image.get_figure()
                fig.savefig('top_search_key_2020.png')
        elif year == '2019':
            print(s5)
            sql = build_sql(year, 5)
            cur.execute(sql)
            rd = cur.fetchall()
            df = pd.DataFrame(rd, columns=['stt', 'keyword', 'sum_val', 'monthly', 'max_val'])
            if len(df):
                image = df.plot.bar(title='TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM 2019')
                fig = image.get_figure()
                fig.savefig('top_search_key_2019.png')

def build_sql(year=2020, limit=10):
        sql = """SELECT row_number() over (ORDER BY A.sum_val DESC, A.keyword, B.monthly) as stt , 
                        A.keyword, A.sum_val, B.monthly, B.max_val
                FROM
                    (	SELECT keyword, sum(VALUE::INT) sum_val
                        FROM vn_trending
                        WHERE EXTRACT(YEAR FROM DATE) = %s
                        GROUP BY keyword
                        ORDER BY sum(VALUE::INT) DESC
                        LIMIT %s
                    ) A
                JOIN 
                    (
                        SELECT DISTINCT A1.keyword, A2.monthly, A1.max_val
                        FROM
                        (	SELECT keyword, max(sum_val) max_val
                            FROM
                            (
                                SELECT DISTINCT keyword, sum(VALUE::INT) sum_val, to_char(date::date,'mm/yyyy') monthly
                                FROM vn_trending
                                WHERE EXTRACT(YEAR FROM DATE) = %s
                                GROUP BY keyword,to_char(date::date,'mm/yyyy') 
                                ORDER BY keyword, sum(VALUE::INT) DESC
                            ) A3 
                            GROUP BY keyword
                        ) A1
                        JOIN
                        (		SELECT DISTINCT keyword, sum(VALUE::INT) sum_val, to_char(date::date,'mm/yyyy') monthly
                                FROM vn_trending
                                WHERE EXTRACT(YEAR FROM DATE) = %s
                                GROUP BY keyword,to_char(date::date,'mm/yyyy') 
                                ORDER BY keyword, sum(VALUE::INT) DESC
                            ) A2 ON A1.keyword = A2.keyword and A1.max_val = A2.sum_val
                    ) B ON A.keyword = B.keyword
                ORDER BY A.sum_val DESC, A.keyword, B.monthly;""" % (year, limit, year, year)

        return sql

if __name__ == '__main__':
    search_key_word()
    top_five_trending('2020')
    top_five_trending('2019')
import pandas as pd
import psycopg2
from pytrends.request import TrendReq
trends = TrendReq()
df = pd.read_excel('keytrends.xlsx', sheet_name='key_word')
# connect to the PostgreSQL server
conn = None
try:
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="1122000",
        host="localhost",
        port="5432")
    cur = conn.cursor()
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
                timeframe='2020-01-01 2020-12-31',
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

    # # create a Pandas Excel writer using XlsxWriter as the engine
    writer = pd.ExcelWriter('vn_trending_result.xlsx', engine='xlsxwriter')
    cur.execute("""
        SELECT *
        FROM(    SELECT keyword, trend_type, SUM(vn_trending.Value) AS totalValue
        from public.vn_trending
        Group by vn_trending.keyword, trend_type) AS query1
        ORDER BY query1.totalValue DESC 
        FETCH  FIRST  10 rows only 
        """)

    db = pd.DataFrame(cur.fetchall())
    db.rename(columns={0: 'keyword', 1: 'trend_type', 2: 'totalValue'}, inplace=True)
    db.index = pd.RangeIndex(start=1, stop=len(db) + 1, step=1)
    db.to_excel(writer, sheet_name='Top 10 key words')
    # # close the Pandas Excel writer and output the Excel file
    writer.save()

    cur.close()
    # commit the changes
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()


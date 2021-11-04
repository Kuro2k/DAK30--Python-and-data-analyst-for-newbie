import pandas as pd
import psycopg2
import sys
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
        CREATE TABLE IF NOT EXISTS vn_trending (
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
        file_name = input("Nhập tên file chứa key trending: ")
        while True:
            try:
                df = pd.read_excel(file_name)
                break
            except:
                file_name = input("File không tồn tại hoặc định dạng chưa đúng.\nVui lòng nhập lại: ")
        from_date = input('Nhâp thời gian bắt đầu theo định dạng yyyy-mm-dd: ')
        to_date = input('Nhập thời gian kết thúc theo định dạng yyyy-mm-đd: ')
        print("Chương trình đang kết nối và lấy dữ liệu từ google trends....")
        date = from_date + ' ' + to_date
        columns_name = list(df.columns)
        for col in columns_name:
            df_ = df[col]
            df_.dropna(inplace=True)
            keywords = df_.values.tolist()
            for kw in keywords:
                pytrends.build_payload(
                    kw_list=[kw],
                    cat=0,
                    timeframe=date,
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
        conn.commit()
        conn.close()
        conn, cur = connect()
        cur.execute("""
                DELETE FROM public.vn_trending u1 USING public.vn_trending u2
                WHERE u1.Id > u2.Id AND u1.keyword = u2.keyword
                    AND u1.date = u2.date AND u1.trend_type = u2.trend_type
        """)

        cur.close()
        # commit the changes
        conn.commit()
        print("Insert dữ liệu thành công")
    except (Exception, psycopg2.DatabaseError) as error:
        print('Insert dữ liệu thất bại')
        print(error)
    finally:
        if conn is not None:
            conn.close()


def query_top10_trending():
    conn, cur = connect()
    cur.execute("""
        SELECT keyword, SUM(Value) AS TotalSearch
        FROM public.vn_trending
        WHERE EXTRACT(YEAR FROM vn_trending.date) = 2020
        GROUP BY keyword
        ORDER BY TotalSearch DESC 
        LIMIT 10 
    """)
    df = pd.DataFrame(cur.fetchall())
    keyword_list = df[0].tolist()
    value_list = df[1].tolist()
    # create Excel file
    workbook = xlsxwriter.Workbook('trending_top_ten.xlsx')
    worksheet = workbook.add_worksheet(name='Top ten trending')
    # add format and title columns
    cellformat = workbook.add_format()
    cellformat.set_center_across()
    cellformat.set_bold()
    worksheet.merge_range('B1:D1', 'Danh sách từ khóa tìm kiếm tại Việt Nam', cellformat)
    worksheet.merge_range('B2:D2', 'Năm 2020', cellformat)
    worksheet.write('A3', 'STT', cellformat)
    worksheet.write('B3', 'Keyword', cellformat)
    worksheet.write('C3', 'Số lần tìm kiếm', cellformat)
    worksheet.write('D3', 'Tháng tìm kiếm\n nhiều nhất', cellformat)
    stt = 1
    row = 3
    for kw in keyword_list:
        worksheet.write(row, 0, stt, cellformat)
        worksheet.write(row, 1, kw)
        worksheet.write(row, 2, value_list[stt - 1])
        cur.execute("""
            SELECT trending2.date2, SUM(trending2.value) AS totalMonth
            FROM (
            SELECT keyword, (EXTRACT(YEAR FROM trending1.date) || '/' || EXTRACT(MONTH FROM trending1.date)) AS date2, 
            trending1.value  
            FROM ( SELECT  keyword, date, value
            FROM  public.vn_trending
            WHERE keyword = %s) AS trending1) AS trending2
            GROUP BY trending2.date2
            ORDER BY totalMonth DESC 
            
        """, [kw])
        date2 = pd.DataFrame(cur.fetchall())
        date2 = date2[0].tolist()
        date_time = datetime.strptime(date2[0], '%Y/%m')
        cellformat2 = workbook.add_format({'num_format': 'mm/yyyy'})
        worksheet.write(row, 3, date_time, cellformat2)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 20)
        worksheet.set_column('D:D', 30)
        row += 1
        stt += 1
    workbook.close()
    cur.close()
    # commit the changes
    conn.commit()
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
    workbook = xlsxwriter.Workbook('trending_search_2020.xlsx')
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
    plt.savefig('trending_top_fiv_2020.png')
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
    plt.savefig('trending_top_fiv_2019.png')
    cur.close()
    # commit the changes
    conn.commit()
    conn.close()


def query_top5_2019_2020():
    conn, cur = connect()
    # create Excel file
    workbook = xlsxwriter.Workbook('trending_top_fiv_2020_2019.xlsx')
    worksheet = workbook.add_worksheet("Top 5 trending 2019 2020")
    cellformat = workbook.add_format()
    cellformat.set_bold()
    cellformat.set_center_across()
    worksheet.merge_range('A1:G1', 'THỐNG KÊ TÌM KIẾM NHIỀU NHẤT TRONG 2 NĂM', cellformat)
    worksheet.merge_range('A2:D2', 'Năm 2020', cellformat)
    worksheet.merge_range('E2:G2', 'Năm 2019', cellformat)
    worksheet.write('A3', 'STT', cellformat)
    worksheet.write('B3', 'Keyword', cellformat)
    worksheet.write('C3', 'Số lần tìm kiếm', cellformat)
    worksheet.write('D3', 'Tháng tìm kiếm nhiều nhất', cellformat)
    worksheet.write('E3', 'Keyword', cellformat)
    worksheet.write('F3', 'Số lần tìm kiếm', cellformat)
    worksheet.write('G3', 'Tháng tìm kiếm nhiều nhất', cellformat)
    row = 3
    cur.execute("""
            SELECT keyword, SUM(Value) AS TotalSearch
            FROM public.vn_trending
            WHERE EXTRACT(YEAR FROM vn_trending.date) = 2020
            GROUP BY keyword
            ORDER BY TotalSearch DESC 
            LIMIT 5 
        """)
    trending_2020 = pd.DataFrame(cur.fetchall())
    cur.execute("""
                SELECT keyword, SUM(Value) AS TotalSearch
                FROM public.vn_trending
                WHERE EXTRACT(YEAR FROM vn_trending.date) = 2019
                GROUP BY keyword
                ORDER BY TotalSearch DESC 
                LIMIT 5 
            """)
    trending_2019 = pd.DataFrame(cur.fetchall())
    for walker in range(0,5):
        worksheet.write(row, 0, walker + 1, cellformat)
        worksheet.write(row, 1, trending_2020[0][walker])
        worksheet.write(row, 2, trending_2020[1][walker])
        cur.execute("""
            SELECT trending2.date2, SUM(trending2.value) AS totalMonth
            FROM (
            SELECT keyword, (EXTRACT(YEAR FROM trending1.date) || '/' || EXTRACT(MONTH FROM trending1.date)) AS date2, 
            trending1.value  
            FROM ( SELECT  keyword, date, value
            FROM  public.vn_trending
            WHERE keyword = %s AND EXTRACT(YEAR FROM vn_trending.date) = 2020) AS trending1) AS trending2
            GROUP BY trending2.date2
            ORDER BY totalMonth DESC 
        """, [trending_2020[0][walker]])
        most_month_2020 = pd.DataFrame(cur.fetchall())
        date_time = datetime.strptime(most_month_2020[0][0], '%Y/%m')
        cellformat2 = workbook.add_format({'num_format': 'mm/yyyy'})
        worksheet.write(row, 3, date_time, cellformat2)
        worksheet.write(row, 4, trending_2019[0][walker])
        worksheet.write(row, 5, trending_2019[1][walker])
        cur.execute("""
                    SELECT trending2.date2, SUM(trending2.value) AS totalMonth
                    FROM (
                    SELECT keyword, (EXTRACT(YEAR FROM trending1.date) || '/' || EXTRACT(MONTH FROM trending1.date)) AS date2, 
                    trending1.value  
                    FROM ( SELECT  keyword, date, value
                    FROM  public.vn_trending
                    WHERE keyword = %s AND EXTRACT(YEAR FROM vn_trending.date) = 2019) AS trending1) AS trending2
                    GROUP BY trending2.date2
                    ORDER BY totalMonth DESC 
                """, [trending_2019[0][walker]])
        most_month_2019 = pd.DataFrame(cur.fetchall())
        date_time = datetime.strptime(most_month_2019[0][0], '%Y/%m')
        cellformat2 = workbook.add_format({'num_format': 'mm/yyyy'})
        worksheet.write(row, 6, date_time, cellformat2)
        row += 1
    worksheet.set_column('B:G', 20)
    worksheet.set_column('G:G', 30)
    worksheet.set_column('D:D', 30)
    workbook.close() # save file

if __name__ == '__main__':
    while True:
        s1 = '\n 1. Lấy dữ liệu trending từ file\n'
        s2 = '2. Xuất báo cáo top 10 trending\n'
        s3 = '3. Xuất báo cáo search keyword in 2020\n'
        s4 = '4. Vẽ biểu đồ line chart top 5 trending các từ khóa tìm kiếm nhiều nhất 2020\n'
        s5 = '5. Vẽ biểu đồ bar chart top 5 trending các từ khóa tìm kiếm nhiều nhất 2019\n'
        s6 = '6. Thống kê tìm kiếm top trending 5 từ khóa trong 2 năm 2020, 2019\n'
        s0 = '...............\n'
        s99 = '99. Thoát\n'

        print(s1, s2, s3, s4, s5, s6, s0, s99)
        require = input("Nhập số tương ứng công việc cần thực hiện: ")
        try:
            require = int(require)
        except:
            require = 0
        while require < 1 or (require > 6 and require < 99) or require > 99:
            require = input("Nhập sai! Xin mời nhập lại: ")
            try:
                require = int(require)
            except:
                require = 0
        if require == 1:
            input_data()
            print('\n----------------------')
        elif require == 2:
            query_top10_trending()
            print('Yêu cầu được thực hiện thành công.')
            print('\n----------------------')
        elif require == 3:
            write_report()
            print('Yêu cầu được thực hiện thành công.')
            print('\n----------------------')
        elif require == 4:
            query_top_2020()
            print('Yêu cầu được thực hiện thành công.')
            print('\n----------------------')
        elif require == 5:
            query_top_2019()
            print('Yêu cầu được thực hiện thành công.')
            print('\n----------------------')
        elif require == 6:
            query_top5_2019_2020()
            print("Yêu cầu được thực hiện thành công.")
            print('\n----------------------')
        elif require == 99:
            print("Chương trình đã kết thúc!")
            sys.exit()


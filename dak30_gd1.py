import pandas as pd
import xlsxwriter
import xlwt
import psycopg2
from pytrends.request import TrendReq
trends = TrendReq()
df = pd.read_excel('keytrends.xlsx', sheet_name='key_word')
# create a Pandas Excel writer using XlsxWriter as the engine
writer = pd.ExcelWriter('vn_trend_2020.xlsx', engine='xlsxwriter')
for col_name in df.columns:
    keywords = pd.DataFrame(df[col_name])
    keywords = df[col_name].dropna().astype(str).values.tolist()
    length = len(keywords)
    total_Data = pd.DataFrame()
    for walker in range(0, length, 5):
        trends.build_payload(
            kw_list=keywords[walker:walker + 5 if(walker + 5 < length) else length],
            cat=0,
            timeframe="2020-01-01 2020-12-31",
            geo="VN",
            gprop=''
        )
        data = trends.interest_over_time()
        if not data.empty:
            data = data.drop(labels=['isPartial'], axis='columns')  # Xoa cot "isPartial" do lu lieu = false
        total_Data = pd.concat([total_Data, data], axis='columns')
    total_Data.to_excel(writer, sheet_name=col_name.replace('/', '_'))
# close the Pandas Excel writer and output the Excel file
writer.save()

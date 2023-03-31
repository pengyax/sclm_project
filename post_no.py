import pandas as pd
from sql_engine import connect
import save_table_to_access as sv
import mail_finance as m

year = '2021'
month = '05'
df1 = pd.read_excel(f'.\\{year}{month}\\邮费\\销售单查询{month}.xlsx')
df2 = pd.read_excel(f'.\\{year}{month}\\邮费\\出库单{month}.xlsx')
df1 = df1[['销售渠道','网店订单号','订单编号','发货仓库','物流单号','发货时间']]
with pd.ExcelWriter(f'.\\{year}{month}\\邮费\\吉客云{month}发货信息.xlsx') as writer:
    
    df1.to_excel(writer, sheet_name='销售发货',index=False)
    df2.to_excel(writer, sheet_name='非销售出库',index=False)
writer.save()
writer.close()
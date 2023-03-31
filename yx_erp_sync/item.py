import pandas as pd
from sql_engine import connect


yx_item = '''
select DISTINCT trans_no SourceNo,title from oryx_account.account_trans_record
'''
yx_con, server = connect('yx')
yx_item_data = pd.read_sql(yx_item,yx_con)


erp_item = '''
select DISTINCT IIF (
		( SourceNo IS NULL OR LEN( SourceNo ) = 0 ) 
		AND OrderNo LIKE 'SCBA%',
		OrderNo,
		SourceNo 
	) SourceNo,
	item 
	from 
	SCLMERPDB_UE.dbo.DH_BalanceApply 
'''
erp_con = connect('new_ERP')
erp_item_data = pd.read_sql(erp_item,erp_con)
data = pd.merge(yx_item_data,erp_item_data,how='outer',on='SourceNo')
data = data[['title','item']].drop_duplicates()
print(data)
data.to_excel('11.xlsx',index=None)




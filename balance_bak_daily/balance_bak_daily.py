from sql_engine import connect
import datetime
import pandas as pd

def trans_date(date,diff):
    end_date = date
    diff_date = datetime.timedelta(diff)
    start_date = end_date - diff_date
    end_date = end_date.strftime('%Y-%m-%d')
    start_date = start_date.strftime('%Y-%m-%d')
    return start_date,end_date

def load_balance_data(date,diff,con):
    start_date,end_date = trans_date(date,diff)
    balance_SQL = f'''
    SELECT
		AgentId,
		AgentWechat,
		AgentName,
		ac.SellerName,
		ActivityName,
		ActivityId,
		Balance,
		CONVERT ( VARCHAR ( 10 ), baktime, 23 ) CreateTime 
	FROM
		DH_TopUserBalance_Bak_Daily201201 TB
		LEFT JOIN SCLMERPDB_UE.dbo.DH_Activity AC ON TB.ActivityId= AC.F_Id 
	WHERE
		1 = 1 
		AND CONVERT ( VARCHAR ( 10 ), baktime, 23 ) BETWEEN '{start_date}' and '{end_date}'
		AND Balance != 0 
		AND ActivityCWTypeNo = 1 
		AND TB.SellerNo NOT IN ( '800018', '800003' ) 
		AND (
			AgentWechat NOT LIKE '%测试%' 
			AND agentwechat NOT LIKE '%test%' 
			AND agentname NOT LIKE '%测试%' 
			AND agentname NOT LIKE '%test%' 
			AND agentname NOT LIKE '%测试程%' 
			AND agentname NOT LIKE '%黄明亮%' 
		)
	UNION ALL
	SELECT
		userid,
		WeChat,
		RealName,
		a.SellerName,
		b.zcname,
		b.zc_id,
		CAST ( round( amt_total / 100, 2 ) AS NUMERIC ( 10, 2 ) ) balance,
		CONVERT ( VARCHAR ( 10 ), CreateTime, 23 ) CreateTime 
	FROM
		[101.132.73.94].SCLM_DRPDB.dbo.A_SybBalanceStock a
		LEFT JOIN bidata.bidata.dbo.ZCLX b ON a.activityid = b.zc_id 
	WHERE
		CONVERT ( VARCHAR ( 10 ), CreateTime, 23 ) BETWEEN '{start_date}' and '{end_date}'
		AND (
			RealName NOT LIKE '%测试%' 
			AND RealName NOT LIKE '%test%' 
			AND RealName NOT LIKE '%测试%' 
			AND RealName NOT LIKE '%test%' 
			AND RealName NOT LIKE '%测试程%' 
			AND RealName NOT LIKE '%黄明亮%' 
		) 
		AND b.zctype = '混拿回款' 
		AND amt_total != 0  UNION ALL
	SELECT
		AgentId,
		AgentWechat,
		AgentName,
		ac.SellerName,
		ActivityName,
		ActivityId,
		Balance,
		CONVERT ( VARCHAR ( 10 ), baktime, 23 ) CreateTime 
	FROM
		[106.15.90.171].JKERPDB_UE.dbo.DH_TopUserBalance_Dail_Bak TB
		LEFT JOIN SCLMERPDB_UE.dbo.DH_Activity AC ON TB.ActivityId= AC.F_Id 
	WHERE
		1 = 1 
		AND CONVERT ( VARCHAR ( 10 ), baktime, 23 ) BETWEEN '{start_date}' and '{end_date}'
		AND Balance != 0 
		AND ActivityCWTypeNo = 1 
		AND (
			AgentWechat NOT LIKE '%测试%' 
			AND agentwechat NOT LIKE '%test%' 
			AND agentname NOT LIKE '%测试%' 
			AND agentname NOT LIKE '%test%' 
			AND agentname NOT LIKE '%测试程%' 
		AND agentname NOT LIKE '%黄明亮%' 
		)
		'''
    balance_data = pd.read_sql(balance_SQL, con)
    return balance_data
    
def contrast(date,diff,con):
    balance_data = load_balance_data(date,diff,con)
    todo = set(balance_data['CreateTime'].astype('str').to_list())
    bi_con = connect('bidata')
    if len(todo)>0:
        todo = "('"+"','".join(todo)+"')"
        bi_con.execute(f"DELETE FROM InterMediate_balance_bak_daily_erp WHERE CreateTime IN {todo}")
    if len(balance_data)>0:
        balance_data.to_sql("InterMediate_balance_bak_daily_erp", bi_con, index=False, if_exists='append', method='multi', chunksize=100)
    bi_con.dispose()
  
def main():
    con = connect('new_ERP')
    date = datetime.datetime.today().date()
    contrast(date,3,con)

if __name__=='__main__':
    main()
    
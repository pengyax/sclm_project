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
    balance_SQL = f'''WITH balance AS (
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
		))
		
		select c.UnionNumber,a.* from (
		SELECT
		isnull(t1.AgentId,t2.AgentId) AgentId,
		isnull(t1.AgentWechat,t2.AgentWechat) AgentWechat,
		isnull(t1.AgentName,t2.AgentName) AgentName,
		isnull(t1.ActivityId,t2.ActivityId) ActivityId,
		isnull(t1.ActivityName,t2.ActivityName) ActivityName,
		isnull(t1.balance,0) / 10000 beg_balance,
		isnull(t2.balance,0) / 10000 end_balance,
		(isnull(t2.balance,0)- isnull(t1.balance,0)) /10000 balance_diff,
		t1.CreateTime beg_time,
		isnull(t1.CreateTime,CONVERT ( VARCHAR ( 10 ), DATEADD( DAY, - 1, t2.CreateTime ), 23 )) CreateTime,
		t2.CreateTime end_time
	FROM
		balance t1
		full JOIN ( SELECT AgentId,AgentName,AgentWechat,ActivityId,ActivityName, CONVERT ( VARCHAR ( 10 ), DATEADD( DAY, - 1, CreateTime ), 23 ) prev_time, CreateTime, Balance FROM balance ) t2 ON t1.CreateTime = t2.prev_time and t1.AgentId = t2.AgentId and t1.ActivityId = t2.ActivityId
		where 
		1=1 
	-- 	and isnull(t1.CreateTime,CONVERT ( VARCHAR ( 10 ), DATEADD( DAY, - 1, t2.CreateTime ), 23 )) = '2021-04-13'
	-- 	and isnull(t2.balance,0)- isnull(t1.balance,0) != 0	
 ) a 
		LEFT JOIN SCLM_DRPDB.[dbo].b_UserInfo c ON a.AgentId = c.f_id
		'''
    balance_data = pd.read_sql(balance_SQL, con)
    return balance_data
    
   
   
    

def load_return_shipment_data(date,diff,con):
    start_date,end_date = trans_date(date,diff)
    return_shipment_SQL = f'''SELECT 
	auditime,WeChat,UnionNumber,RealName,zcname,
	sum(returned_money) as returned_money,
	sum(ship_money) as ship_money,
	sum(returned_money) + sum(ship_money) money
FROM
(

SELECT CONVERT
	( VARCHAR ( 10 ), DateNum, 23 ) AS auditime,
	UI.WeChat,
	UI.UnionNumber,
	UI.realname,
	b.zcname,
	returned_money + JK_returned_money returned_money,
	0 AS ship_money 
FROM
	bidata.bidata.dbo.InterMediate_WS_agent_zc_daily a
	LEFT JOIN bidata.bidata.dbo.ZCLX b ON a.zc_id = b.zc_id 
	left join .dbo.b_UnionUserInfo_FromDRP UI on a.UnionNumber = UI.UnionNumber
WHERE
	DateNum BETWEEN '{start_date}' and '{end_date}'
	AND b.zctype = '混拿回款' 
	and UI.UnionNumber is not null
	and returned_money + JK_returned_money != 0
	
UNION all 

SELECT
	auditime,
	AgentWechat,
	b.UnionNumber,
	AgentName,
	zcname,
	SUM ( flowprice ) AS money,
	0 AS ship_money 
FROM
	(
	SELECT 
	
		CONVERT( VARCHAR ( 10 ), auditime, 120 ) auditime,
		AgentWechat,
		Agentid,
		AgentName,
		b.zcname,
		( InValue - OutValue ) / 10000 AS flowprice 
	FROM
		dbo.DH_BalanceApply a
		LEFT JOIN [bidata].[bidata].dbo.ZCLX b ON a.ActivityID = b.zc_id 
	WHERE
		zctype = '混拿回款' 
		AND status = 1 
		AND Deleted = 0 
		AND (
			Item IN ( '货物预存款', '客户余额互转', '余额清理', '政策余额充值', '政策余额转款', '三合一转款', '全品牌转出', '代理清算', '全品牌A转B', '卡位收支', '全品牌转入', '全品牌转款' ) 
			OR ( Item IN ( '政策赠送', '退赠送金额' ) AND ActivityName LIKE '%意向金%' ) 
		) 
		AND agentname NOT LIKE '%测试%' 
		AND agentname NOT LIKE '%test%' 
		AND agentname NOT LIKE '%测试程%' 
		AND agentname NOT LIKE '%黄明亮%' 
		AND CONVERT ( VARCHAR ( 10 ), auditime, 120 ) BETWEEN '{start_date}' and '{end_date}'
		AND DATEDIFF( dd, auditime, getdate( ) ) = 0 

		
		UNION ALL
		
		
	SELECT 
		
		CONVERT( VARCHAR ( 10 ), auditime, 120 ) auditime,
		AgentWechat,
		Agentid,
		AgentName,
		b.zcname,
		( InValue - OutValue ) / 10000 AS flowprice 
	FROM
		JKERPDB_UE.dbo.DH_BalanceApply a
		LEFT JOIN [bidata].[bidata].dbo.ZCLX b ON a.ActivityID = b.zc_id 
	WHERE
		zctype = '混拿回款' 
		AND status = 1 
		AND Deleted = 0 
		AND (
			Item IN ( '货物预存款', '客户余额互转', '余额清理', '政策余额充值', '政策余额转款', '三合一转款', '全品牌转出', '全品牌A转B', '代理清算', '卡位收支', '全品牌转入', '全品牌转款' ) 
			OR ( Item IN ( '政策赠送', '退赠送金额' ) AND ActivityName LIKE '%意向金%' ) 
		) 
		AND agentname NOT LIKE '%测试%' 
		AND agentname NOT LIKE '%test%' 
		AND agentname NOT LIKE '%测试程%' 
		AND agentname NOT LIKE '%黄明亮%' 
		AND CONVERT ( VARCHAR ( 10 ), auditime, 120 ) BETWEEN '{start_date}' and '{end_date}'
		AND DATEDIFF( dd, auditime, getdate( ) ) = 0 
	) a 
	LEFT JOIN DH_UserInfo b on a.agentid = b.id
GROUP BY
	auditime,
	AgentWechat,
	AgentName,
	zcname,
	UnionNumber
	
UNION all
	
	SELECT
	sendtime,
	agentWechat,
	UnionNumber,
	agentname,
	zcname,
	0 AS money,
	SUM ( money ) AS ship_money 
FROM
	(
SELECT 
		CONVERT( VARCHAR ( 10 ), auditime, 120 ) sendtime,
		a.AgentWechat,
		a.Agentid,
		a.AgentName,
		b.zcname,
		( InValue - OutValue ) / 10000 AS money 
	FROM
		dbo.DH_BalanceApply a
		LEFT JOIN [bidata].[bidata].dbo.ZCLX b ON a.ActivityID = b.zc_id
		join [dbo].[DH_Order] c on a.SourceNo = c.id
	WHERE
		zctype = '混拿回款' and b.zcname not in ('全品牌-2021开门红财富峰会')
		AND status = 1 
		AND Deleted = 0 
		and c.orderstatus > 200
		AND a.agentname NOT LIKE '%测试%' 
		AND a.agentname NOT LIKE '%test%' 
		AND a.agentname NOT LIKE '%测试程%' 
		AND a.agentname NOT LIKE '%黄明亮%' 
		AND CONVERT ( VARCHAR ( 10 ), auditime, 120 ) BETWEEN '{start_date}' and '{end_date}'
	
union all 

	SELECT 
		CONVERT( VARCHAR ( 10 ), auditime, 120 ) sendtime,
		a.AgentWechat,
		a.Agentid,
		a.AgentName,
		b.zcname,
		( InValue - OutValue ) / 10000 AS money 
	FROM
		dbo.DH_BalanceApply a
		LEFT JOIN [bidata].[bidata].dbo.ZCLX b ON a.ActivityID = b.zc_id
		join [dbo].[DH_Order] c on a.SourceNo = c.id
	WHERE
		zctype = '混拿回款' and b.zcname not in ('全品牌-2021开门红财富峰会')
		AND status = 1 
		AND Deleted = 0 
		and c.orderstatus = 102
		AND a.agentname NOT LIKE '%测试%' 
		AND a.agentname NOT LIKE '%test%' 
		AND a.agentname NOT LIKE '%测试程%' 
		AND a.agentname NOT LIKE '%黄明亮%' 
		AND CONVERT ( VARCHAR ( 10 ), auditime, 120 ) BETWEEN '{start_date}' and '{end_date}'
		

	) a LEFT JOIN dbo.DH_UserInfo b ON a.agentid = b.id 
GROUP BY
sendtime,agentWechat,agentname,zcname,UnionNumber
	
	UNION ALL
	
	SELECT
	sendtime,
	agentWechat,
	UnionNumber,
	agentname,
	zcname,
	0 AS money,
	SUM ( money ) AS ship_money 
FROM
	(
SELECT 
		CONVERT( VARCHAR ( 10 ), auditime, 120 ) sendtime,
		a.AgentWechat,
		a.Agentid,
		a.AgentName,
		b.zcname,
		( InValue - OutValue ) / 10000 AS money 
	FROM
		JKERPDB_UE.dbo.DH_BalanceApply a
		LEFT JOIN [bidata].[bidata].dbo.ZCLX b ON a.ActivityID = b.zc_id
		join [dbo].[DH_Order] c on a.SourceNo = c.id
	WHERE
		zctype = '混拿回款' and b.zcname not in ('全品牌-2021开门红财富峰会')
		AND status = 1 
		AND Deleted = 0 
		and c.orderstatus > 200
		AND a.agentname NOT LIKE '%测试%' 
		AND a.agentname NOT LIKE '%test%' 
		AND a.agentname NOT LIKE '%测试程%' 
		AND a.agentname NOT LIKE '%黄明亮%' 
		AND CONVERT ( VARCHAR ( 10 ), auditime, 120 ) BETWEEN '{start_date}' and '{end_date}'
	
union all 

	SELECT 
		CONVERT( VARCHAR ( 10 ), auditime, 120 ) sendtime,
		a.AgentWechat,
		a.Agentid,
		a.AgentName,
		b.zcname,
		( InValue - OutValue ) / 10000 AS money 
	FROM
		JKERPDB_UE.dbo.DH_BalanceApply a
		LEFT JOIN [bidata].[bidata].dbo.ZCLX b ON a.ActivityID = b.zc_id
		join [dbo].[DH_Order] c on a.SourceNo = c.id
	WHERE
		zctype = '混拿回款' and b.zcname not in ('全品牌-2021开门红财富峰会')
		AND status = 1 
		AND Deleted = 0 
		and c.orderstatus = 102
		AND a.agentname NOT LIKE '%测试%' 
		AND a.agentname NOT LIKE '%test%' 
		AND a.agentname NOT LIKE '%测试程%' 
		AND a.agentname NOT LIKE '%黄明亮%' 
		AND CONVERT ( VARCHAR ( 10 ), auditime, 120 ) BETWEEN '{start_date}' and '{end_date}'
		

	) a LEFT JOIN dbo.DH_UserInfo b ON a.agentid = b.id 
	
GROUP BY
sendtime,agentWechat,agentname,zcname,UnionNumber
) a
where
1=1
GROUP BY auditime,WeChat,RealName,zcname,UnionNumber
		'''
    return_shipment_data = pd.read_sql(return_shipment_SQL, con)
    return return_shipment_data

def contrast(date,diff,con):
    balance_data = load_balance_data(date,diff,con)
    return_shipment_data = load_return_shipment_data(date,diff,con)
    data = pd.merge(balance_data, return_shipment_data, how='outer', left_on=['UnionNumber','CreateTime','ActivityName','AgentWechat'],right_on=['UnionNumber','auditime','zcname','WeChat'],suffixes=('_b', '_rs'))
    data['err_type'] = None
    data.loc[data.CreateTime.isnull(),'err_type'] = '回款下单有余额没有'
    data.loc[data.auditime.isnull(),'err_type'] = '余额有回款下单没有'
    data['balance_diff'].fillna(0,inplace = True)
    data['money'].fillna(0,inplace = True)
    data.loc[(data['err_type'].isnull())&((data['balance_diff']-data['money']).abs()>0),'err_type'] = '金额不一致'
    data['trans_time'] = data['CreateTime']
    data.loc[data['CreateTime'].isnull(),'trans_time'] = data['auditime']
    data['diff'] = data['balance_diff'] - data['money']
    data = data[data.err_type.notnull()]
    todo = set(data['trans_time'].astype('str').to_list())
    bi_con = connect('bidata')
    if len(todo)>0:
        todo = "('"+"','".join(todo)+"')"
        bi_con.execute(f"DELETE FROM InterMediate_balance_returned_shipment_money_diff WHERE trans_time IN {todo}")
    if len(data)>0:
        data.to_sql("InterMediate_balance_returned_shipment_money_diff", bi_con, index=False, if_exists='append', method='multi', chunksize=500)
    bi_con.dispose()

def main():
    con = connect('new_ERP')
    date = datetime.datetime.today().date()
    target_date = datetime.datetime.strptime('2021-03-25','%Y-%m-%d')
    contrast(date,5,con)

if __name__=='__main__':
    main()
    
    
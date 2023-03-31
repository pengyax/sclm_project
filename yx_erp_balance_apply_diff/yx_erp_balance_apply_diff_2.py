from sql_engine import connect
import datetime
import pandas as pd


def check_diff_days(diff_day):
    today = datetime.datetime.today()
    diff_day = datetime.timedelta(days=diff_day)
    prev_day = today-diff_day
    interval = today-prev_day
    days = interval.days
    begin_signal = datetime.datetime(2020, 10, 16)
    if prev_day < begin_signal:
        return (today-begin_signal).days
    else:
        return days


def load_yx_balance_apply_data(diff_days_start, diff_days_end, yx_con):
    diff_days_start = check_diff_days(diff_days_start)
    diff_days_end = check_diff_days(diff_days_end)
    balance_apply_SQL = f'''
	
SELECT
		CAST(
		t1.id AS CHAR ( 30 )) id,
		t1.trans_time,
		LEFT ( t1.trans_time, 10 ) trans_date,
		t1.trans_no SourceNo,
		CAST(
		t3.user_id AS CHAR ( 30 )) user_id,
		t3.union_id UnionNumber,
		t3.name name,
		t2.out_id activityId,
        t2.policy_name,
		t1.title,
		t1.body,
	IF
		( t1.amt_trans > 0, t1.amt_trans / 100, 0 ) amt_in,
	IF
		( t1.amt_trans < 0, t1.amt_trans / 100, 0 ) amt_out,
	IF
		( t1.amt_trans > 0, t1.amt_trans / 100, 0 ) +
	IF
		( t1.amt_trans < 0, t1.amt_trans / 100, 0 ) money
	FROM
		oryx_account.account_trans_record t1
		INNER JOIN oryx_policy.policy t2 ON t1.policy_id = t2.policy_id
		INNER JOIN oryx_agent.agent_info t3 ON t1.customer_id = t3.user_id
		left join oryx_bi.zclx t6 on t2.out_id = t6.zc_id
		
	WHERE
		t1.company_id IN ( 12, 979 ) 
		AND t6.ActivityCWTypeNo in (1,2)
		and t2.state = 1
		AND t1.customer_type = 0 
		AND t1.account_type = 0 
		AND ( t1.type NOT IN ( 'pay', 'refund' ) OR t1.title = '余额申请单' ) 
		AND t1.title NOT IN ( '历史订单取消发货单余额退款', '车票审核补报上账', '历史订单余额退款', '车票审核报销上账' ) 
		AND DATEDIFF( NOW(), t1.trans_time )<={diff_days_start} AND DATEDIFF(NOW(), t1.trans_time)>={diff_days_end}
		AND t1.policy_id IS NOT NULL 
		AND ( t1.remark NOT LIKE '%%测试%%' OR t1.remark IS NULL ) 
		and title = '余额申请单'
		AND t3.union_id not in ('CM66381516','CM00006317','CM00007650')
                                        '''
    balance_apply_data = pd.read_sql(balance_apply_SQL, yx_con)
    return balance_apply_data


def load_erp_balance_apply_data(diff_days_start, diff_days_end, erp_con):
    diff_days_start = check_diff_days(diff_days_start)
    diff_days_end = check_diff_days(diff_days_end)
    balance_apply_SQL = f'''SELECT
	iif (a.PayAccount = '','无账号',a.PayAccount) merchant_id,
	a.applytime auditime,
	CONVERT(varchar(100), a.applytime, 23) audi_date,
	IIF (
		( a.SourceNo IS NULL OR LEN( a.SourceNo ) = 0 ) 
		AND a.OrderNo LIKE 'SCBA%',
		a.OrderNo,
		a.SourceNo 
	) SourceNo,
	IIF (
		a.agentId= 'dab70acc-9b11-44e9-bc95-d6ad1e573336',
		'CM00000035',
		ISNULL(
			IIF (
				b.UnionNumber= 'CM88810886',
				'CM00020001',
				IIF (
					b.UnionNumber= 'CM00007497',
					'CM00007482',
					IIF (
						b.UnionNumber= 'CM00007595',
						'CM00007113',
						IIF ( b.UnionNumber= 'CM00006816', 'CM00006194', IIF ( b.UnionNumber= 'CM00007136', 'CM00000304', b.UnionNumber ) ) 
					) 
				) 
			),
			c.UnionNumber 
		) 
	) UnionNumber,
	isnull(c.realname,b.RealName) realname,
	d.zcname,
	item,
	InValue,
	- OutValue OutValue,
	InValue - OutValue money
FROM
	SCLMERPDB_UE.dbo.DH_BalanceApply a
	LEFT JOIN SCLM_DRPDB.dbo.b_UserInfo b ON a.agentId= b.f_id
	LEFT JOIN SCLM_DRPDB.dbo.b_UnionUserInfo c ON b.UnionNumber= c.UnionNumber 
	AND isdelete = 0
	LEFT JOIN dbo.ZCLX d ON a.activityid = d.zc_id 
WHERE
	a.status = 1 
	AND Deleted = 0 
	AND datediff( dd, ISNULL( applytime, a.createTime ), getdate( ) ) <= { diff_days_start } 
	AND datediff( dd, ISNULL( applytime, a.createTime ), getdate( ) ) >= { diff_days_end }
	AND d.ActivityCWTypeNo in (1,2)
	AND agentname NOT LIKE '%%测试%%' 
	and c.UnionNumber not in ('CM00006317','CM00007650')
	AND Item in ('货物预存款','全品牌转款','销售订单撤销')
	and a.remark like '%%余额申请%%'
	AND agentwechat NOT IN ( 'Power', 'manna24', '13076952970', 'zhulei666', 'test05' )
 	-- and iif (a.PaymentAccount = '','无账号',a.PaymentAccount) != '无账号'
 '''
    balance_apply_data = pd.read_sql(balance_apply_SQL, erp_con)
    return balance_apply_data


def contrast_balance_apply(diff_days_start, diff_days_end):
    yx_con, server = connect('yx')
    erp_con = connect('new_ERP')
    bi_con = connect('bidata')
    yx_balance_apply_data = load_yx_balance_apply_data(
        diff_days_start, diff_days_end, yx_con)
    erp_balance_apply_data = load_erp_balance_apply_data(
        diff_days_start, diff_days_end, erp_con)
    yx_con.dispose()
    server.close()
    erp_con.dispose()
    data = pd.merge(yx_balance_apply_data[yx_balance_apply_data.SourceNo.notnull()], erp_balance_apply_data[erp_balance_apply_data.SourceNo.notnull()], how='outer', left_on=[
                    'SourceNo', 'UnionNumber', 'trans_date', 'policy_name'], right_on=['SourceNo', 'UnionNumber', 'audi_date', 'zcname'], suffixes=('_yx', '_erp'))
    data['err_type'] = None
    data.loc[data.trans_date.isnull(), 'err_type'] = 'ERP有YX没有'
    data.loc[data.audi_date.isnull(), 'err_type'] = 'YX有ERP没有'
    data['money_yx'].fillna(0, inplace=True)
    data['money_erp'].fillna(0, inplace=True)
    data['diff'] = data['money_yx'] - data['money_erp']
    data.loc[(data['err_type'].isnull()) & (
        (data['money_yx']-data['money_erp']).abs() > 0), 'err_type'] = '金额不一致'
    data['date'] = data['trans_date']
    data.loc[data['trans_date'].isnull(), 'date'] = data['audi_date']
    data = data[data.err_type.notnull()]
    data.reset_index(drop=True, inplace=True)
    todo = set(data['trans_date'].astype('str').to_list())
    if len(todo) > 0:
        todo = "('"+"','".join(todo)+"')"
        bi_con.execute(
            f"DELETE FROM InterMediate_yx_erp_balance_apply_diff WHERE date IN {todo}")
    if len(data) > 0:
    	data.to_sql("InterMediate_yx_erp_balance_apply_diff", bi_con,index=False, if_exists='append', method='multi', chunksize=500)
    bi_con.dispose()


def main():
    contrast_balance_apply(60, 0)


if __name__ == '__main__':
    main()

from sql_engine import *
import datetime
import pandas as pd




def load_erp_order_detail(diff_days_start, diff_days_end, erp_con):
    shipment_SQL = f'''SELECT
	c.platformOrderNo order_no,
    c.createType,
	c.activityId,
    e.UnionNumber,
	CONVERT ( VARCHAR ( 10 ), a.sendtime, 23 ) AS sendtime,
	SUM(b.sendCount) sendCount,
	SUM ( ( d.totalAmount- isnull( d.apportionAmount, 0 ) ) / totalCount * b.count ) AS money 
FROM
	SCLMERPDB_UE.dbo.DH_Order_Shipment a
	LEFT JOIN SCLMERPDB_UE.dbo.DH_Order_Shipments_Relation b ON a.shipNo= b.shipNo
	LEFT JOIN SCLMERPDB_UE.dbo.DH_Order c ON a.orderNo= c.orderNo 
	AND b.orderNo= c.orderNo
	LEFT JOIN SCLMERPDB_UE.dbo.DH_Order_Detail d ON c.orderNo= d.orderNo 
	AND b.orderDetailNo= d.orderDetailNo
	LEFT JOIN SCLMERPDB_UE.dbo.DH_UserInfo e ON a.agentId=e.id
-- 	LEFT JOIN dbo.ZCLX e ON c.activityid = e.zc_id 
WHERE
	a.status= 80 
	AND len( a.agentid ) > 0 
	AND a.isDeleted= 0 
	AND b.isdeleted=0 
	AND c.isdeleted= 0 
	AND c.orderstatus > 200 
	AND len( c.orderNo ) > 0 
-- 	AND e.zctype = '混拿回款' 
	AND datediff( dd, a.sendtime, getdate( ) ) <= {diff_days_start} AND datediff( dd, a.sendtime, getdate( ) ) >= {diff_days_end}
GROUP BY
	c.platformOrderNo,
    c.createType,
	e.UnionNumber,
	c.activityId,
	CONVERT ( VARCHAR ( 10 ), a.sendtime, 23 ) '''
    shipment_data = pd.read_sql(shipment_SQL, erp_con)
    return shipment_data


def load_yx_order_detail(diff_days_start, diff_days_end, yx_con):
    shipment_SQL = f'''SELECT LEFT
	( t8.created_time, 10 ) sendtime,
	t1.order_no,
    t5.user_id,
	t5.union_id UnionNumber,
	t7.out_id activityId,
	SUM( t8.release_num ) ship_num,
	SUM( t8.release_num * t3.price )/ 100 ship_amount
FROM
	oryx_order.order_info t1
-- LEFT JOIN oryx_order.order_log t2 ON t1.order_id = t2.order_id
	LEFT JOIN oryx_order.order_item t3 ON t1.order_id = t3.order_id
	LEFT JOIN oryx_goods.goods t4 ON t3.goods_id = t4.goods_id
	LEFT JOIN oryx_agent.agent_info t5 ON t1.user_id = t5.user_id
	LEFT JOIN oryx_account.account_trans_record t6 ON t1.order_no = t6.trans_no
	LEFT JOIN oryx_policy.policy t7 ON t6.policy_id = t7.policy_id 
	LEFT JOIN oryx_logistics.delivery_order_goods_record t8 ON t3.id=t8.order_item_id
WHERE
	DATEDIFF( NOW(), t8.created_time )<={diff_days_start}
	AND DATEDIFF( NOW(), t8.created_time )>={diff_days_end}
	AND t1.push_state = 1 
	AND t1.company_id IN (12, 979)
	AND t1.order_state >= 3 
-- AND t2.order_state = 3 
-- AND order_state_before = 2 
	AND t1.deleted_at IS NULL 
	AND t3.goods_name NOT LIKE '%%测试%%' 
	AND t6.policy_id IS NOT NULL 
	AND t6.company_id IN (12, 979)
    AND t6.type='pay'
GROUP BY
	LEFT ( t8.created_time, 10 ),
	t1.order_no,
    t5.user_id,
	t5.union_id,
	t7.out_id'''
    shipment_data = pd.read_sql(shipment_SQL, yx_con)
    return shipment_data


def load_yx_refund_data(diff_days_start, diff_days_end, yx_con):
    refund_SQL = f'''SELECT
                                FROM_UNIXTIME( t9.t_created, '%%Y-%%m-%%d %%H:%%i:%%S' ) date,
                                t1.state,
                                CAST(t1.service_id AS CHAR) service_id,
                                CAST(t5.order_id AS CHAR) order_id,
                                t5.order_no,
                                CAST(t6.user_id AS CHAR) user_id,
                                t6.union_id,
                                t8.out_id activityId,
                                t3.customized_no goodsNo,
                                t2.n_submited refund_num,
                                t2.n_submited * t3.price/100 refund_amount 
                            FROM
                                `after_service` t1
                                LEFT JOIN after_service_item t2 ON t1.service_id = t2.after_service_id
                                LEFT JOIN order_item t3 ON t1.order_id = t3.order_id 
                                AND t2.order_item_id = t3.id 
                                AND t2.ex_spec_id = t3.spec_id
                                LEFT JOIN order_info t5 ON t3.order_id = t5.order_id
                                LEFT JOIN oryx_goods.goods t4 ON t3.goods_id = t4.goods_id
                                LEFT JOIN oryx_agent.agent_info t6 ON t5.user_id = t6.user_id
                                LEFT JOIN oryx_account.account_trans_record t7 ON t5.order_no = t7.trans_no AND t7.type='refund' AND title='售后退款'
                                LEFT JOIN oryx_policy.policy t8 ON t7.policy_id = t8.policy_id 
                                INNER JOIN after_service_communicate t9 ON t1.service_id=t9.service_id AND t9.feedback=202
                            WHERE
                                DATEDIFF(
                                    NOW(),
                                FROM_UNIXTIME( t9.t_created, '%%Y-%%m-%%d' ))<{diff_days_start}
                                AND DATEDIFF(
                                    NOW(),
                                FROM_UNIXTIME( t1.t_created, '%%Y-%%m-%%d' ))>={diff_days_end}
                                AND t1.company_id = 12 
                                AND t1.service_type = 'returned' 
                                -- AND t1.state = 'completed' 
                                AND t1.is_deleted = 0 
                                AND t3.goods_name NOT LIKE '%%测试%%' 
                                -- AND t7.policy_id IS NOT NULL 
                                '''
    refund_data = pd.read_sql(refund_SQL, yx_con)
    return refund_data


def load_erp_refund_data(diff_days_start, diff_days_end, erp_con):
    refund_SQL = f'''SELECT
                                a.createdTime,
                                b.auditeTimeStorage AS auditeTime,
                                a.platformOrderNo order_no,
                                d.UnionNumber,
                                a.activityId,
                                b.goodsNo,
                                refundCount,
                                drawbackvalue  AS money ,
                                CAST(a.refundFlag AS char(2)) refundFlag
                            FROM
                                SCLMERPDB_UE.dbo.DH_AfterSale a
                                LEFT JOIN SCLMERPDB_UE.dbo.DH_AfterSale_Details b ON a.AfterNo = b.AfterNo
                                LEFT JOIN SCLMERPDB_UE.dbo.DH_Order c ON a.OrderNo=c.OrderNo
                                LEFT JOIN SCLM_DRPDB.dbo.b_UserInfo d ON a.agentId = d.F_Id
                            WHERE
                            -- 	a.refundFlag = 88 
                            -- 	AND 
                                b.refuntype = '退货' 
                                AND datediff( dd, ISNULL(b.auditeTimeStorage, a.createdTime), getdate( ) ) < {diff_days_start} AND datediff( dd, ISNULL(b.auditeTimeStorage, a.createdTime), getdate( ) ) >= {diff_days_end}  
                                '''
    refund_data = pd.read_sql(refund_SQL, erp_con)
    return refund_data


def contrast_refund(diff_days_start, diff_days_end):
    yx_con, server = connect('yx')
    erp_con = connect()
    yx_data = load_yx_refund_data(diff_days_start, diff_days_end, yx_con)
    erp_data = load_erp_refund_data(diff_days_start, diff_days_end, erp_con)
    yx_con.dispose()
    server.close()
    erp_con.dispose()
    yx_data = goodsNo_format(yx_data, '_')
    erp_data = goodsNo_format(erp_data, '_')
    yx_data = goodsNo_format(yx_data, '-')
    erp_data = goodsNo_format(erp_data, '-')    
    data = pd.merge(yx_data, erp_data, how='outer', on=['order_no', 'goodsNo'])
    data['temp1'] = None
    data['temp2'] = None
    data['temp3'] = None
    data['temp4'] = None
    data['temp5'] = None
    data['temp1'][data.date.isnull()] = 'ERP有YX没有'
    data['temp2'][data.UnionNumber.isnull()] = 'YX有ERP没有'
    data['temp3'][((data.state=='completed')&(data.refundFlag!='88'))|(data.refundFlag=='22')] = 'ERP退款状态异常'
    data['temp4'][data.refund_num!=data.refundCount] = '退货数量不一致'
    data['temp5'][data.refund_amount!=data.money] = '退货金额不一致'
    if len(data)>0:
        data['err_type'] = data.apply(lambda x: [i for i in x[['temp1', 'temp2', 'temp3', 'temp4', 'temp5']].to_list() if i ], axis=1)
        for i in ['temp1','temp2','temp3','temp4','temp5']:
            data.drop(i, axis=1, inplace=True)
        data['err_type'][data['err_type'].str.contains('ERP有YX没有')] = 'ERP有YX没有'
        data['err_type'][data['err_type'].str.contains('YX有ERP没有')] = 'YX有ERP没有'
        data['err_type'] = data['err_type'].apply(lambda x:', '.join(x))
        if len(data)>0:
            data.to_sql("InterMediate_yx_erp_refund_unsync_daily", connect('bidata'), index=None, if_exists='append', method='multi', chunksize=100)
    return


def goodsNo_format(df, to_split):
    d1 = df[df.goodsNo.str.contains(to_split)]
    d2 = df[df.goodsNo.str.contains(to_split)==False]
    d1['goodsNo'] = d1['goodsNo'].str.extract(f'(.+){to_split}')
    df = pd.concat([d1,d2])
    return df


def contrast(diff_days_start, diff_days_end):
    yx_con, server = connect('yx')
    erp_con = connect()
    yx_data = load_yx_order_detail(diff_days_start, diff_days_end, yx_con)
    erp_data = load_erp_order_detail(diff_days_start, diff_days_end, erp_con)
    data = yx_data.merge(erp_data, how='outer', on=['sendtime', 'order_no', 'activityId'], suffixes=('_from_yx','_from_erp'))
    yx_con.dispose()
    server.close()
    erp_con.dispose()
    data = data[((data.ship_num!=data.sendCount)|((data.ship_amount-data.money).abs()>5))&(data.createType=='HGT')]
    data['err_type'] = ''
    data['err_type'][data['ship_amount'].isnull()] = 'ERP有YX没有'
    data['err_type'][data['money'].isnull()] = 'YX有ERP没有'
    data['err_type'][(((data.ship_amount-data.money).abs()>5)|(data.ship_num!=data.sendCount))&(data.err_type=='')] = '发货数量/金额不一致'
    data['err_type'][(data.UnionNumber_from_yx!=data.UnionNumber_from_erp)&(data.UnionNumber_from_erp.notnull())&(data.UnionNumber_from_yx.notnull())] = 'UN不一致'
    data = data.where(data.notnull(), None)
    data['user_id'] = data['user_id'].apply(lambda x: str(int(x)) if x else None)
    bi_con = connect('bidata')
    try:
        bi_con.execute(f"""DELETE FROM InterMediate_yx_erp_shipment_unsync_daily WHERE sendtime IN ('{"','".join(set(data.sendtime))}')""")
    except Exception:
        pass
    data.to_sql("InterMediate_yx_erp_shipment_unsync_daily", bi_con, index=None, if_exists='append', method='multi', chunksize=100)
    return


def load_yx_charge_data(diff_days_start, diff_days_end, yx_con):
    diff_days_start = check_diff_days(diff_days_start)
    diff_days_end = check_diff_days(diff_days_end)
    charge_SQL = f'''SELECT
                                        CAST(t1.id AS CHAR(30)) id,
                                        t1.trans_time,
                                        t1.trans_no SourceNo,
                                        CAST(t3.user_id AS CHAR(30)) user_id,
                                        t3.union_id UnionNumber,
                                        t2.out_id activityId,
                                        t1.type,
                                        t1.title,
                                        t1.body,
                                        IF(t1.amt_trans>0, t1.amt_trans/100, 0) amt_in,
                                        IF(t1.amt_trans<0, t1.amt_trans/100, 0) amt_out
                                    FROM
                                        oryx_account.account_trans_record t1
                                        INNER JOIN
                                        oryx_policy.policy t2 ON t1.policy_id=t2.policy_id
                                        INNER JOIN
                                        oryx_agent.agent_info t3 ON t1.customer_id=t3.user_id
                                    WHERE
                                        t1.company_id IN (12, 979)
                                        AND t2.business_type='hunna'
                                        AND t1.customer_type = 0 
                                        AND t1.account_type =0
                                        AND(t1.type NOT IN ('pay', 'refund') OR t1.title='余额申请单')
                                        AND t1.title NOT IN ('历史订单取消发货单余额退款', '车票审核补报上账', '历史订单余额退款', '车票审核报销上账')
                                        AND DATEDIFF( NOW(), t1.trans_time )<={diff_days_start} AND DATEDIFF(NOW(), t1.trans_time)>={diff_days_end}
                                        AND t1.policy_id IS NOT NULL
                                        AND (t1.remark NOT LIKE '%%测试%%' OR t1.remark IS NULL)
                                        AND t3.union_id!='CM66381516'
                                        '''
    charge_data = pd.read_sql(charge_SQL, yx_con)
    return charge_data


def check_diff_days(diff_day):
    today = datetime.datetime.today()
    diff_day = datetime.timedelta(days=diff_day)
    prev_day = today-diff_day
    interval = today-prev_day
    days = interval.days
    begin_signal = datetime.datetime(2021, 1, 16)
    if prev_day<begin_signal:
        return (today-begin_signal).days
    else:
        return days


def load_erp_charge_data(diff_days_start, diff_days_end, erp_con):
    diff_days_start = check_diff_days(diff_days_start)
    diff_days_end = check_diff_days(diff_days_end)
    charge_SQL = f'''SELECT
                                a.OrderNo,
                                        IIF(a.agentId='dab70acc-9b11-44e9-bc95-d6ad1e573336', 'CM00000035',
                                        ISNULL(
                                            IIF(b.UnionNumber='CM88810886', 'CM00020001', 
                                            IIF(b.UnionNumber='CM00007497', 'CM00007482',
                                            IIF(b.UnionNumber='CM00007595','CM00007113', 
                                            IIF(b.UnionNumber='CM00006816', 'CM00006194', 
                                            IIF(b.UnionNumber='CM00007136', 'CM00000304', b.UnionNumber))))), c.UnionNumber)) UnionNumber,
                                        a.activityId,
                                        IIF((a.SourceNo IS NULL OR LEN(a.SourceNo)=0) AND a.OrderNo LIKE 'SCBA%', a.OrderNo, a.SourceNo) SourceNo,
                                        ISNULL(auditime, a.createTime) auditime,
                                        InValue,
                                        -OutValue OutValue,
                                        Item
                                FROM
                                        SCLMERPDB_UE.dbo.DH_BalanceApply a
                                        LEFT JOIN
                                        SCLM_DRPDB.dbo.b_UserInfo b
                                        ON a.agentId=b.f_id
                                        LEFT JOIN
                                        SCLM_DRPDB.dbo.b_UnionUserInfo c
                                        ON b.UserPhone=c.UserPhone AND isdelete=0
                                      LEFT JOIN dbo.ZCLX d ON a.activityid = d.zc_id 
                                WHERE
                                        a.status = 1 
                                        AND Deleted = 0 
                                        AND datediff( dd, ISNULL(auditime, a.createTime), getdate( ) ) <= {diff_days_start} AND datediff( dd, ISNULL(auditime, a.createTime), getdate( ) ) >= {diff_days_end}
                                      AND d.zctype = '混拿回款' 
                                        AND agentname NOT LIKE '%测试%' 
                                        AND 
                                        -- (
                                                        Item IN ( '货物预存款', '客户余额互转', '余额清理', '政策余额充值', '政策余额转款', '三合一转款', '全品牌转出', '代理清算', '卡位收支', '全品牌转入', '全品牌转款', '订单补差额', '全品牌A转B','销售订单撤销', '政策赠送', '退赠送金额', '保证金收支') 
                                           --             OR ( Item IN ( '政策赠送', '退赠送金额' ) AND ActivityName LIKE '%意向金%' ) 
                                          --      ) 
                                        AND agentwechat NOT IN ( 'Power', 'manna24', '13076952970', 'zhulei666', 'test05' )'''
    charge_data = pd.read_sql(charge_SQL, erp_con)
    return charge_data


def contrast_charge(diff_days_start, diff_days_end):
    yx_con, server = connect('yx')
    erp_con = connect()
    yx_charge_data = load_yx_charge_data(diff_days_start, diff_days_end, yx_con)
    erp_charge_data = load_erp_charge_data(diff_days_start, diff_days_end, erp_con)
    yx_charge_data['id'] = yx_charge_data['id'].str.strip()
    yx_charge_data['user_id'] = yx_charge_data['user_id'].str.strip()
    yx_charge_data['tp'] = 1
    yx_charge_data['tp'][yx_charge_data['amt_out']!=0] = 0
    erp_charge_data['tp'] = 1
    erp_charge_data['tp'][erp_charge_data['OutValue']!=0] = 0
    yx_con.dispose()
    server.close()
    erp_con.dispose()
    data = pd.merge(yx_charge_data[yx_charge_data.SourceNo.notnull()], erp_charge_data[erp_charge_data.SourceNo.notnull()], how='outer', on=['SourceNo', 'UnionNumber', 'activityId', 'tp'])
    data['err_type'] = None
    data['err_type'][data.id.isnull()] = 'ERP有YX没有'
    data['err_type'][data.OrderNo.isnull()] = 'YX有ERP没有'
    data['err_type'][(data['err_type'].isnull())&(((data['InValue']-data['amt_in']).abs()>1)|((data['OutValue']-data['amt_out']).abs()>1))] = '金额不一致'
    data['err_type'][(data['Item'].isin(['销售订单撤销', '政策赠送', '退赠送金额', '保证金收支']))&(data['err_type']=='ERP有YX没有')] = None
    data = data[data.err_type.notnull()]
    if len(data)>0:
        bi_con = connect('bidata')
        data.to_sql("InterMediate_yx_erp_charge_unsync_daily", bi_con, index=None, if_exists='append', method='multi', chunksize=100)
    return


def load_yx_order_data(diff_days_start, diff_days_end=1):
    yx_con, server = connect('yx')
    bi_con = connect('bidata')
    shipment_SQL = f"""
SELECT LEFT
	( t8.created_time, 10 ) date,
    t5.user_id,
	t5.union_id,
	t7.out_id,
	SUM( t8.release_num ) ship_num,
	SUM( t8.release_num * t3.price )/ 100 ship_amount,
	COUNT( DISTINCT t3.order_id ) shipment_order_num 
FROM
	oryx_order.order_info t1
-- LEFT JOIN oryx_order.order_log t2 ON t1.order_id = t2.order_id
	LEFT JOIN oryx_order.order_item t3 ON t1.order_id = t3.order_id
	LEFT JOIN oryx_goods.goods t4 ON t3.goods_id = t4.goods_id
	LEFT JOIN oryx_agent.agent_info t5 ON t1.user_id = t5.user_id
	LEFT JOIN oryx_account.account_trans_record t6 ON t1.order_no = t6.trans_no
	LEFT JOIN oryx_policy.policy t7 ON t6.policy_id = t7.policy_id 
	LEFT JOIN oryx_logistics.delivery_order_goods_record t8 ON t3.id=t8.order_item_id
WHERE
	DATEDIFF( NOW(), t8.created_time )<={ diff_days_start } 
	AND DATEDIFF( NOW(), t8.created_time )>={ diff_days_end } 
	AND t1.push_state = 1 
	AND t1.company_id = 12 
	AND t1.order_state >= 3 
-- AND t2.order_state = 3 
-- AND order_state_before = 2 
	AND t1.deleted_at IS NULL 
	AND t3.goods_name NOT LIKE '%%测试%%' 
	AND t6.policy_id IS NOT NULL 
	AND t6.company_id = 12 
    AND t6.type='pay'
GROUP BY
	LEFT ( t8.created_time, 10 ),
    t5.user_id,
	t5.union_id,
	t7.out_id
                                        """
    refund_SQL = f"""
SELECT
	FROM_UNIXTIME( t1.t_handler, '%%Y-%%m-%%d' ) date,
    t6.user_id,
	t6.union_id,
	t8.out_id,
	SUM( t2.n_submited ) refund_num,
	SUM( t2.n_submited * t3.price ) refund_amount 
FROM
	`after_service` t1
	LEFT JOIN after_service_item t2 ON t1.service_id = t2.after_service_id
	LEFT JOIN order_item t3 ON t1.order_id = t3.order_id 
	AND t2.order_item_id = t3.id 
	AND t2.ex_spec_id = t3.spec_id
	LEFT JOIN order_info t5 ON t3.order_id = t5.order_id
	LEFT JOIN oryx_goods.goods t4 ON t3.goods_id = t4.goods_id
	LEFT JOIN oryx_agent.agent_info t6 ON t5.user_id = t6.user_id
	LEFT JOIN oryx_account.account_trans_record t7 ON t5.order_no = t7.trans_no
	LEFT JOIN oryx_policy.policy t8 ON t7.policy_id = t8.policy_id 
WHERE
	DATEDIFF(
		NOW(),
	FROM_UNIXTIME( t1.t_handler, '%%Y-%%m-%%d' ))<={ diff_days_start } 
	AND DATEDIFF(
		NOW(),
	FROM_UNIXTIME( t1.t_handler, '%%Y-%%m-%%d' ))>={ diff_days_end } 
	AND t1.company_id = 12 
	AND t1.service_type = 'returned' 
	AND t1.state = 'completed' 
	AND t1.is_deleted = 0 
	AND t3.goods_name NOT LIKE '%%测试%%' 
	AND t7.policy_id IS NOT NULL 
	AND t7.company_id = 12 
GROUP BY
	FROM_UNIXTIME( t1.t_handler, '%%Y-%%m-%%d' ),
    t6.user_id,
	t6.union_id,
	t8.out_id
    """
    sales_SQL = f"""
SELECT LEFT
	( FROM_UNIXTIME( t1.t_pay_time ), 10 ) date,
    t1.user_id,
	t5.union_id,
	t7.out_id,
	SUM( t3.num ) sale_num,
	SUM( t3.num * t3.price )/ 100 sale_amount,
	COUNT( DISTINCT t3.order_id ) order_num 
FROM
	oryx_order.order_info t1
	LEFT JOIN order_item t3 ON t1.order_id = t3.order_id
	LEFT JOIN oryx_goods.goods t4 ON t3.goods_id = t4.goods_id
	LEFT JOIN oryx_agent.agent_info t5 ON t1.user_id = t5.user_id
	LEFT JOIN oryx_account.account_trans_record t6 ON t1.order_no = t6.trans_no
	LEFT JOIN oryx_policy.policy t7 ON t6.policy_id = t7.policy_id 
WHERE
	DATEDIFF(
		NOW(),
	FROM_UNIXTIME( t1.t_pay_time ))<= {diff_days_start} 
	AND DATEDIFF(
		NOW(),
	FROM_UNIXTIME( t1.t_pay_time ))>= {diff_days_end}
	AND t1.push_state = 1 
	AND t1.company_id = 12 
	AND t1.pay_state IN ( 2, 3 ) 
	AND t1.deleted_at IS NULL 
	AND t3.goods_name NOT LIKE '%%测试%%' 
	AND t6.policy_id IS NOT NULL 
	AND t6.company_id = 12 
GROUP BY
	LEFT ( FROM_UNIXTIME( t1.t_pay_time ), 10 ),
    t1.user_id,
	t5.union_id,
	t7.out_id
    """
    return_money_SQL = f"""
                                        SELECT
                                        CAST(DATE(t1.trans_time) AS CHAR(10)) date,
                                        t3.user_id,
                                        t3.union_id,
                                        t2.out_id,
                                        SUM( IF(t1.amt_trans>0, t1.amt_trans/100, 0) ) amt_in,
                                        SUM( IF(t1.amt_trans<0, t1.amt_trans/100, 0) ) amt_out
                                    FROM
                                        oryx_account.account_trans_record t1
                                        INNER JOIN
                                        oryx_policy.policy t2 ON t1.policy_id=t2.policy_id
                                        INNER JOIN
                                        oryx_agent.agent_info t3 ON t1.customer_id=t3.user_id
                                    WHERE
                                        t1.company_id = 12 
                                        AND t1.customer_type = 0 
                                        AND t1.account_type =0
                                        AND DATEDIFF( NOW(), t1.trans_time )<={diff_days_start} AND DATEDIFF(NOW(), t1.trans_time)>={diff_days_end}
                                        AND t1.policy_id IS NOT NULL
                                    GROUP BY 
                                        DATE(t1.trans_time),
                                        t3.user_id,
                                        t3.union_id,
                                        t2.out_id
    """
    shipments = pd.read_sql(shipment_SQL, yx_con)
    refunds = pd.read_sql(refund_SQL, yx_con)
    sales = pd.read_sql(sales_SQL, yx_con)
    return_money = pd.read_sql(return_money_SQL, yx_con)
    data = pd.merge(shipments, refunds, how='outer', on=['date', 'user_id', 'union_id', 'out_id']).merge(sales, how='outer', on=['date', 'user_id', 'union_id', 'out_id']).merge(return_money, how='outer', on=['date', 'user_id', 'union_id', 'out_id'])
    for i in ['ship_num', 'ship_amount',
       'shipment_order_num', 'refund_num', 'refund_amount', 'sale_num',
       'sale_amount', 'order_num', 'amt_in', 'amt_out']:
       data[i].fillna(0, inplace=True)
    yx_con.dispose()
    server.close()
    data.columns = ['DateNum', 'user_id', 'UnionNumber', 'zc_id', 'shipment_num',
       'recivable', 'shipment_order_num', 'refund_num', 'refund',
       'place_goods_num', 'order_amount', 'place_order_num',
       'returned_money_in', 'returned_money_out']
    data['money'] = data['recivable']+data['refund']
    data = data[['DateNum', 'user_id', 'UnionNumber', 'zc_id',
       'recivable', 'refund', 'money', 'shipment_num', 'shipment_order_num', 'refund_num',
       'place_goods_num', 'order_amount', 'place_order_num',
       'returned_money_in', 'returned_money_out']]
    bi_con.execute(f"DELETE FROM InterMediate_WS_agent_zc_daily_from_yx WHERE DATEDIFF(DD, DateNum, GETDATE()) BETWEEN {diff_days_end} AND {diff_days_start}")
    todo = set(data['DateNum'].astype('str').to_list())
    if len(todo)>0:
        todo = "('"+"','".join(todo)+"')"
        bi_con.execute(f"DELETE FROM InterMediate_WS_agent_zc_daily_from_yx WHERE DateNum IN {todo}")
    # data = data[data.err_type.str.len()>0]
    data.to_sql("InterMediate_WS_agent_zc_daily_from_yx", bi_con, index=None, if_exists='append', method='multi', chunksize=1000)
    bi_con.dispose()
    return


if __name__=='__main__':
    load_yx_order_data(1, 1)
    contrast(1, 1)
    contrast_refund(1, 1)
    contrast_charge(1, 1)
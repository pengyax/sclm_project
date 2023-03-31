from sql_engine import connect
import pandas as pd


def generate_SQL(diff_day_start, diff_day_end=0):
    WMS_SQL = f"""SELECT
                                t1.SOREFERENCE1,
                                t1.SOREFERENCE2,
                                t2.SKU,
                                t2.QTYSHIPPED,
                                t2.QTYSHIPPED_EACH,
                                t2.LINESTATUS,
                                t1.ORDERTIME
                            	-- t3.CHANGETIME
                            FROM
                                WMS_USER.DOC_ORDER_HEADER t1
                                LEFT JOIN WMS_USER.DOC_ORDER_DETAILS t2 ON t1.ORDERNO = t2.ORDERNO
                            	-- LEFT JOIN WMS_USER.IDX_ORDERSTATUS_LOG t3 ON t1.ORDERNO=t2.ORDERNO AND t3.STATUS IN (70, 80)
                            WHERE
                                t1.CUSTOMERID = '100001' 
                                AND t1.SOREFERENCE1 NOT LIKE 'SCWSA%'
                                AND t1.SOREFERENCE1 NOT LIKE 'SCCGT%'
                                AND TO_DATE(TO_CHAR(SYSDATE, 'yyyy-mm-dd'), 'yyyy-mm-dd')-ORDERTIME BETWEEN {diff_day_end} AND {diff_day_start}
                                AND t2.LINESTATUS IN (70, 80, 98, 99)
                                AND t1.CONSIGNEEID NOT IN ('谢喜艳', '郑国贤')
                                """

    ERP_SQL = f"""
                        SELECT
                            a.ShipNo,
                            a.OrderNo,
                            d.goodsNo,
                            e.zc_id,
                            e.zctype,
                            b.sendCount,
                            b.count count,
                            d.totalCount,
                            a.sendtime AS sendtime,
                            c.createShipTime,
                            CONVERT(VARCHAR(5), a.status) status
                        FROM
                            SCLMERPDB_UE.dbo.DH_Order_Shipment a
                            LEFT JOIN SCLMERPDB_UE.dbo.DH_Order_Shipments_Relation b ON a.shipNo= b.shipNo
                            LEFT JOIN SCLMERPDB_UE.dbo.DH_Order c ON a.orderNo= c.orderNo 
                            AND b.orderNo= c.orderNo
                            LEFT JOIN SCLMERPDB_UE.dbo.DH_Order_Detail d ON c.orderNo= d.orderNo 
                            AND b.orderDetailNo= d.orderDetailNo
                            LEFT JOIN bidata.bidata.dbo.ZCLX e ON c.activityid = e.zc_id
                        WHERE
                            len( a.agentid ) > 0 
                            AND a.isDeleted= 0 
                            AND b.isDeleted= 0 
                            AND c.orderstatus > 200 
                            AND len( c.orderNo ) > 0 
                            AND datediff( dd, ISNULL(c.firstPayTime, a.SyncScanTime), getdate( ) ) <= {diff_day_start} AND datediff( dd, ISNULL(c.firstPayTime, a.SyncScanTime), getdate( ) ) > {diff_day_end}
                            AND a.warehouseNo IN ('SH01', 'WH01')
                            AND a.agentName NOT IN ('谢喜艳', '郑国贤')
                            """
    return WMS_SQL, ERP_SQL


def load_data(diff_day_start, diff_day_end=0):
    WMS_SQL, ERP_SQL = generate_SQL(diff_day_start, diff_day_end)
    d_WMS = pd.read_sql(WMS_SQL, connect('wms'))
    d_WMS['key'] = d_WMS['SOREFERENCE1']+d_WMS['SOREFERENCE2']+d_WMS['SKU']
    d_ERP = pd.read_sql(ERP_SQL, connect())
    d_ERP['key'] = d_ERP['ShipNo']+d_ERP['OrderNo']+d_ERP['goodsNo']
    d = pd.merge(d_WMS, d_ERP, how='left', on='key')
    d['LINESTATUS'][d['LINESTATUS'].astype('int')>80] = '80'
    d['status'][d['status'].fillna('0').astype('int')>80] = '80'
    d['err_type'] = None
    d['temp1'] = None
    d['temp2'] = None
    d['temp3'] = None
    d['temp4'] = None
    d['temp1'][d['SOREFERENCE1'].isnull()] = 'ERP发货WMS未发货'
    d['temp2'][d['ShipNo'].isnull()] = 'WMS发货ERP未发货'
    d['diff_num'] = (d['QTYSHIPPED']-d['sendCount']).abs()
    d['temp3'][d['diff_num']>0] = '发货数量不一致'
    d['temp4'][d['LINESTATUS']!=d['status']] = '发货状态不一致'
    d = d[(d.temp1.notnull())|(d.temp2.notnull())|(d.temp3.notnull())|(d.temp4.notnull())]
    d['err_type'] = d.apply(lambda x:','.join([i for i in x[['temp1', 'temp2', 'temp3', 'temp4']] if i]), axis=1)
    d = d[[i for i in d.columns if 'temp' not in i]]
    d = d[d['err_type'].notnull()]
    save_to_db(d)
    return


def save_to_db(df):
    df.to_sql("InterMediate_wms_erp_shipment_unsync_daily", connect('bidata'), index=None, if_exists='append', method='multi', chunksize=100)
    return


if __name__=='__main__':
    load_data(1)
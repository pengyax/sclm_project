import zipfile
import pandas as pd
from sql_engine import connect
import numpy as np
import save_table_to_access as sv
import mail_finance as m

conn_offline = connect('offline')

def format_string(df):
    for i in df.dtypes.to_dict().items():
        if i[1] == np.dtype('O'):
            try:
                df[i[0]] = df[i[0]].apply(lambda x: x.encode(
                    'latin1').decode('GBK') if x != None else None)
            except UnicodeEncodeError:
                df[i[0]]
    return df

def offline(type, year1, year2, month, diff_month):
    if type == '回款':
        file_name = f"./{year2}{diff_month}/线下回款{year2}{diff_month}01.xlsx"
        sql = f"""select '余额' as Account,c.PayAcccount,a.InOutType as Type,a.BillId as TradeID,a.CustomerName as Customer,a.activityID as projectid,a.activityName as ActivityName,d.procatno,
    a.CustomerClass as Shop,a.Item,c.Cause,replace(a.CustomerClass,'微信','') as brand,
    OutValue+InValue as MONEY,a.CustomerID,a.Time as AuditTime from dt_PreDeposit a left join dt_ActivityInfoMng b  on a.activityID=b.ActivityID left join dt_PolicyCat d on d.actcategory=b.actcategory
    left join dt_PredepositApply c on a.BillId=c.BalanceID
    where (a.Item='余额申请' and c.ActivityID=7 and a.Time between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000')
    or (a.CustomerClass in('CS渠道','加盟渠道','直营店') and a.Item='余额申请' and c.ActivityID!=7
    and a.Time between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000')"""

    elif type == '加盟':
        file_name = f"./{year2}{diff_month}/加盟渠道回款{year2}{diff_month}01.xlsx"
        sql = f"""select '余额' as Account,a.sellerAccount as PayAcccount,a.InOutType as Type,a.BillId as TradeID,a.CustomerName as Customer,a.activityID as projectid,a.activityName as ActivityName,d.procatno,
    a.CustomerClass as Shop,a.Item as Item,'' as Cause ,replace(a.CustomerClass,'微信','') as brand,
    OutValue+InValue as MONEY,a.CustomerID,a.Time as AuditTime,e.IdNo from dt_PreDeposit a left join dt_ActivityInfoMng b on a.activityID=b.ActivityID
    left join dt_PolicyCat d on d.actcategory=b.actcategory
    left join dt_Customer e on e.CustomerId=a.CustomerId and e.CustomerClass=a.CustomerClass
    where a.CustomerClass='加盟渠道' and a.Item != '余额申请'
    and a.Time between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000' """

    elif type == '直营':
        file_name = f"./{year2}{diff_month}/直营渠道回款{year2}{diff_month}01.xlsx"
        sql = f"""select '余额' as Account,a.PayAcccount as PayAcccount,a.InOutType as Type,a.BillId as TradeID,a.CustomerName as Customer,a.activityID as projectid,a.activityName as ActivityName,d.procatno,
    a.CustomerClass as Shop,a.Item as Item,'' as Cause,replace(a.CustomerClass,'微信','') as brand,
    OutValue+InValue as MONEY,a.CustomerID,a.Time as AuditTime from dt_PreDeposit a left join dt_ActivityInfoMng b on a.activityID=b.ActivityID left join dt_PolicyCat d on d.actcategory=b.actcategory
    where (a.Item = '余额申请'  and SUBSTRING(a.BillId,1,3)!='YES' and a.activityID=114 and a.activityName='直营店' and  a.paytime is null and a.CustomerID  in(select CustomerID from dt_Customer where CustomerClass='直营店')
    and a.Time between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000' ) """

    elif type == '未发货':
        file_name = f"./{year2}{diff_month}/线下系统已下单未发货明细{year2}{diff_month}01.xlsx"
        sql = f"""SELECT '商品销售' as Reason, '出' as InorOut,'10' as WarehouseNO,t1.TradeNo ,t1.CustomerID as CustomerID,t1.CustomerName as CustomerName,t1.CustomerClass as Shop,t3.BrandName as brand,
    t2.Goodsno,t3.Goodsname, t2.Spec,t2.[count],round(t2.total,2) as total,t1.ActivityID as projectid,t4.ActivityName,t5.procatno,t1.SndTime as AuditTime
    from dt_Trade_Active as  t1 left join dt_TradeGoods_Active as t2 on t1.tradeActiveid=t2.tradeActiveid
    left join dt_GoodsDC as t3 on t2.goodsno=t3.goodsno left join dt_ActivityInfoMng as t4 on t1.ActivityID=t4.ActivityID left join dt_PolicyCat as t5 on t5.actcategory=t4.actcategory
    where t1.TradeStatusNo in(101,200,201,202,301,302,303,304,305,306) """

    elif type == '商品库存':
        file_name = f"./{year2}{diff_month}/线下系统商品库存表{year2}{diff_month}01.xlsx"
        sql = f"""select GoodsNo as '商品编号',GoodsName as '名称',Spec as '规格',PriceAver as '成本价',sum(StocksSave+InferiorStocks+RefundStocksGood+RefundStocksBad) as '数量'
    ,WareId as '仓号'
    from dt_GoodsSubWare
    group by GoodsNo,GoodsName,Spec,PriceAver,WareId"""

    elif type == '退货':
        file_name = f"./{year2}{diff_month}/线下系统退货{year2}{diff_month}01.xlsx"
        sql = f"""select '销售退货' as Reason, '入' as InorOut,case t1.Zone when '库存区' then  '10' when '售后次品区' then '11' end as WarehouseNO,t5.TradeNo,
    t5.CustomerID as CustomerID,t5.CustomerName as CustomerName,t5.CustomerClass as Shop,t3.BrandName as brand,
    t2.Goodsno,t3.Goodsname, t2.Spec,t2.[count],round(t2.Total,2) as total,
    t5.ActivityID as projectid,t6.ActivityName,t7.procatno,t1.AuditTime as AuditTime
    from dt_StockOut as  t1 left join dt_StockOut_Detail as t2 on t1.BillId=t2.BillId
    left join dt_Trade_Active as t5 on t1.TradeActiveId=t5.TradeActiveId
    left join dt_ActivityInfoMng as t6 on t6.ActivityID=t5.ActivityID
    left join dt_PolicyCat as t7 on t7.actcategory=t6.actcategory
    left join dt_GoodsDC as t3 on t2.goodsno=t3.goodsno
    left join dt_Customer as t9 on t5.CustomerId = t9.CustomerId
    where t1.Status in('已审核','已入库') and t1.TotalCounts=0 and t1.Reason='销售退货'
    and t1.AuditTime between   '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000'
    UNION all
    select '销售退货' as Reason, '入' as InorOut, case t1.Zone when '库存区' then '10' when '售后次品区' then '11' end as WarehouseNO,t5.TradeNo ,
    t5.CustomerID as CustomerID,t5.CustomerName as CustomerName,t5.CustomerClass as Shop,t3.BrandName as brand,
    t2.Goodsno,t3.Goodsname, t2.Spec,t2.[count],round(t6.GoodsPrice*t2.[Count],2) as total,
    t5.ActivityID as projectid,t7.ActivityName,t8.procatno,t1.AuditTime as AuditTime
    from dt_StockOut as  t1 left join dt_StockOut_Detail as t2 on t1.BillId=t2.BillId
    left join dt_Trade_Active t5 on t1.TradeActiveId=t5.TradeActiveId
    left join dt_ActivityInfoMng as t7 on t7.ActivityID=t5.ActivityID
    left join dt_PolicyCat as t8 on t8.actcategory=t7.actcategory
    left join dt_GoodsDC as t3 on t2.goodsno=t3.goodsno
    left join dt_wxrefundgoods t6 on t6.TradeActiveId=t1.TradeActiveId and t6.GoodsNo=t2.GoodsNo
    left join dt_Customer as t9 on t5.CustomerId = t9.CustomerId
    where t1.Status in('已审核','已入库')
    and t1.TotalCounts>0 and t1.Reason='销售退货'
    and t1.AuditTime between   '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000'"""

    elif type == '余额':
        file_name = f"./{year2}{diff_month}/线下系统余额{year2}{diff_month}01.xlsx"
        sql = f"""select a.CustomerId,a.CustomerName,a.CustomerClass,a.activityID,a.activityName,a.PreDeposit,d.actcategory  from dt_PreDepositForAc  a
    left join dt_ActivityInfoMng b on a.activityID=b.ActivityID
    left join dt_PolicyCat d on d.actcategory=b.actcategory"""

    elif type == '直营调拨':
        file_name = f"./{year2}{diff_month}/直营调拨单{year2}{diff_month}01.xlsx"
        sql = f"""select '商品销售' as Reason,t1.TradeNo as '订单号' ,t1.CustomerID as '微信号',t1.CustomerClass as '平台',t3.BrandName as '品牌',
            t2.Goodsno as '产品编号',t3.Goodsname as '产品名称', t2.Spec as '规格',t2.[count] as '数量',round(t2.total,2) as '金额',t1.ActivityID as '政策编号',t4.ActivityName as '政策名称',t1.SndTime as '发货时间'
            from dt_Trade_Active as  t1 left join dt_TradeGoods_Active as t2 on t1.tradeActiveid=t2.tradeActiveid
            left join dt_GoodsDC as t3 on t2.goodsno=t3.goodsno left join dt_ActivityInfoMng as t4 on t1.ActivityID=t4.ActivityID
            where t1.TradeStatusNo in('307','308') and t1.sndtime between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000'
             and (t1.TradeNo like '%DH-21%' ) and t1.CustomerClass = '直营店'"""

    elif type == '出货':
        file_name = f"./{year2}{diff_month}/线下系统出货{year2}{diff_month}01.xlsx"
        sql = f"""select '商品销售' as Reason, '出' as InorOut,'10' as WarehouseNO,t1.TradeNo ,t1.CustomerID as CustomerID,t1.CustomerName as CustomerName,t1.CustomerClass as Shop,t3.BrandName as brand,
    t2.Goodsno,t3.Goodsname, t2.Spec,t2.[count],round(t2.total,2) as total,t1.ActivityID as projectid,t4.ActivityName as ActivityName,t5.procatno,t1.SndTime as AuditTime
    from dt_Trade_Active as  t1 left join dt_TradeGoods_Active as t2 on t1.tradeActiveid=t2.tradeActiveid
    left join dt_GoodsDC as t3 on t2.goodsno=t3.goodsno left join dt_ActivityInfoMng as t4 on t1.ActivityID=t4.ActivityID left join dt_PolicyCat as t5 on t5.actcategory=t4.actcategory
    where t1.TradeStatusNo in(307,308) and (t1.OrderOrig!='调拨单登记' or t1.OrderOrig is null) and t1.sndtime between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000'  """

    elif type == 'access加盟':
        file_name = f"./{year2}{diff_month}/(access)加盟渠道回款{year2}{diff_month}01.xlsx"
        sql = f"""select '余额' as Account,a.InOutType as Type,a.BillId as TradeID,a.CustomerName as Customer,a.activityID as projectid,d.procatno,
    a.CustomerClass as Shop,a.Item as Cause,
    OutValue+InValue as MONEY ,replace(a.CustomerClass,'微信','') as brand,a.CustomerID,a.Time as AuditTime,a.sellerAccount as PayAcccount,e.IdNo from dt_PreDeposit a left join dt_ActivityInfoMng b on a.activityID=b.ActivityID
    left join dt_PolicyCat d on d.actcategory=b.actcategory
    left join dt_Customer e on e.CustomerId=a.CustomerId and e.CustomerClass=a.CustomerClass
    where a.CustomerClass='加盟渠道' and a.Item != '余额申请'
    and a.Time between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000' """

    elif type == 'access直营':
        file_name = f"./{year2}{diff_month}/(access)直营渠道回款{year2}{diff_month}01.xlsx"
        sql = f"""select '余额' as Account,a.InOutType as Type,a.BillId as TradeID,a.CustomerName as Customer,a.activityID as projectid,d.procatno,
    a.CustomerClass as Shop,a.Item as Cause,
    OutValue+InValue as MONEY,replace(a.CustomerClass,'微信','') as brand,a.CustomerID,a.Time as AuditTime,'招行-三草两木商贸' as PayAcccount from dt_PreDeposit a left join dt_ActivityInfoMng b on a.activityID=b.ActivityID left join dt_PolicyCat d on d.actcategory=b.actcategory
    where (a.Item = '余额申请'  and SUBSTRING(a.BillId,1,3)!='YES' and a.activityID=114 and a.activityName='直营店' and  a.paytime is null and a.CustomerID  in(select CustomerID from dt_Customer where CustomerClass='直营店')
    and a.Time between '{year1}-{month}-01 00:00:00.000' and '{year2}-{diff_month}-01 00:00:00.000' )"""

    elif type == 'access库存调整':
        file_name = f"./{year2}{diff_month}/(access)库存调整单据{year2}{diff_month}01.xlsx"
        sql = f"""
    SELECT
    a.Reason,
    '入' InOrOut,
    '10' warehouseNo,
    a.BillId,
    c.py,
    b.GoodsNo,
    b.Name,
    b.Spec,
    b.Count * a.IsRedWord AS COUNT,
    case when b.[Count]*d.PriceAver is NULL then 0 else b.[Count]*d.PriceAver*a.IsRedWord end Total,
    '三草两木' brand,
    a.AuditTime
  FROM
    dbo.dt_StockIN AS a
    LEFT JOIN dbo.dt_StockIN_Detail AS b ON a.BillId= b.BillId
    LEFT JOIN dbo.dt_supplier AS c ON a.supname= c.supname
    left join dbo.dt_GoodsDC d ON b.GoodsNo= d.GoodsNo
  WHERE
    ( a.Status= '已入库' OR a.Status= '已审核' )
    AND AuditTime>'{year1}-{month}-01' AND a.AuditTime<'{year2}-{diff_month}-01'
  UNION ALL
  SELECT
    a.Reason,
    '出' InOrOut,
    '10' warehouseNo,
    a.BillId,
    '' py,
    b.GoodsNo,
    b.Name,
    b.Spec,
    b.Count * a.IsRedWord,
    case when b.[Count]*d.PriceAver is NULL then 0 else b.[Count]*d.PriceAver*a.IsRedWord end Total,
    '三草两木' brand,
    a.AuditTime
  FROM
    dbo.dt_StockOut AS a
    LEFT JOIN dbo.dt_StockOut_Detail AS b ON a.BillId= b.BillId
    left join dbo.dt_GoodsDC d ON b.GoodsNo= d.GoodsNo
  WHERE
    ( a.Status= '已入库' OR a.Status= '已审核' )
    AND AuditTime>'{year1}-{month}-01' AND a.AuditTime<'{year2}-{diff_month}-01'
    """

    elif type == 'access客户':
        file_name = f"./{year2}{diff_month}/access客户{year2}{diff_month}01.xlsx"
        sql = f"""SELECT CustomerId,Name,CustomerClass FROM dt_Customer"""

    df = pd.read_sql(sql, conn_offline)
    df = format_string(df)
    df.to_excel(file_name, index=False)
    return df

def access_Account_in_out(file1, file2, file3, target_file_path):
    df = pd.read_excel(file1)
    order = ['Account', 'Type', 'TradeID', 'Customer', 'projectid', 'procatno',
             'Shop', 'Cause', 'MONEY', 'brand', 'CustomerID', 'AuditTime', 'PayAcccount']
    df1 = df[order]
    df2 = pd.read_excel(file2)
    df3 = pd.read_excel(file3)
    list = [df1, df2, df3]
    pd.concat(list).to_excel(target_file_path, index=False)
    return pd.concat(list)

def access_sales(file1, file2, target_file_path):
    df1 = pd.read_excel(file1, dtype={"procatno": "object"})
    df2 = pd.read_excel(file2, dtype={"procatno": "object"})
    list = [df1, df2]
    df = pd.concat(list)
    df.loc[df['procatno'] == '000', 'total'] = 0
    df = df[['Reason', 'InorOut', 'WarehouseNO', 'TradeNo', 'CustomerID', 'projectid',
             'procatno', 'Shop', 'Goodsno', 'Goodsname', 'Spec', 'count', 'total', 'brand', 'AuditTime']]
    df.to_excel(target_file_path, index=False)
    return df


def zip_files(files, zip_name):
    zip = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
    for file in files:
        print('compressing', file)
        zip.write(file)
    zip.close()
    print ('compressing finished')


def main():
    year1 = 2021
    year2 = 2021
    month = 5
    diff_month = ("00" + str(6))[-2:]
    offline("回款", year1, year2, month, diff_month)
    offline("加盟", year1, year2, month, diff_month)
    offline("直营", year1, year2, month, diff_month)
    offline("未发货", year1, year2, month, diff_month)
    offline("商品库存", year1, year2, month, diff_month)
    offline("退货", year1, year2, month, diff_month)
    offline("余额", year1, year2, month, diff_month)
    offline("出货", year1, year2, month, diff_month)
    offline("直营调拨", year1, year2, month, diff_month)
    offline("access加盟", year1, year2, month, diff_month)
    offline("access直营", year1, year2, month, diff_month)

    access_sales_file1 = f"./{year2}{diff_month}/线下系统出货{year2}{diff_month}01.xlsx"
    access_sales_file2 = f"./{year2}{diff_month}/线下系统退货{year2}{diff_month}01.xlsx"
    access_sales_target_file = f"./{year2}{diff_month}/(access)出货+售后{year2}{diff_month}01.xlsx"

    file1 = f'./{year2}{diff_month}/线下回款{year2}{diff_month}01.xlsx'
    file2 = f'./{year2}{diff_month}/(access)直营渠道回款{year2}{diff_month}01.xlsx'
    file3 = f'./{year2}{diff_month}/(access)加盟渠道回款{year2}{diff_month}01.xlsx'
    target_file_path = f'./{year2}{diff_month}/(access)线下回款{year2}{diff_month}01.xlsx'
    df_access_Account_in_out = access_Account_in_out(
        file1, file2, file3, target_file_path)
    df_access_sales = access_sales(
        access_sales_file1, access_sales_file2, access_sales_target_file)
    df_access_customer = offline("access客户", year1, year2, month, diff_month)
    df_access_Stocks = offline("access库存调整", year1, year2, month, diff_month)

    mdb_name = f'ERPTrans线下{month}月'
    sv.save_table_to_access_db(
        df_access_Account_in_out, mdb_name, 'Account_in_out')
    sv.save_table_to_access_db(df_access_sales, mdb_name, 'Sales')
    sv.save_table_to_access_db(df_access_customer, mdb_name, 'customer')
    sv.save_table_to_access_db(df_access_Stocks, mdb_name, 'Stocks')

    f1 = f'./{year2}{diff_month}/加盟渠道回款{year2}{diff_month}01.xlsx'
    f2 = f'./{year2}{diff_month}/线下回款{year2}{diff_month}01.xlsx'
    f3 = f'./{year2}{diff_month}/线下系统出货{year2}{diff_month}01.xlsx'
    f4 = f'./{year2}{diff_month}/线下系统商品库存表{year2}{diff_month}01.xlsx'
    f5 = f'./{year2}{diff_month}/线下系统退货{year2}{diff_month}01.xlsx'
    f6 = f'./{year2}{diff_month}/线下系统已下单未发货明细{year2}{diff_month}01.xlsx'
    f7 = f'./{year2}{diff_month}/线下系统余额{year2}{diff_month}01.xlsx'
    f8 = f'./{year2}{diff_month}/直营调拨单{year2}{diff_month}01.xlsx'
    f9 = f'./{year2}{diff_month}/直营渠道回款{year2}{diff_month}01.xlsx'
    files = [f1,f2,f3,f4,f5,f6,f7,f8,f9]

    zip_file = f'./{year2}{diff_month}/线下{year2}{month}01.zip'
    #压缩包名字
    zip_files(files, zip_file)

    contents = ["Hi",
                f"请查收{month}月线下数据。"]
    subject = f"{month}月线下数据"
    # 发送邮件 yag.send("收件人",邮件标题,邮件内容)
    attach1 = f"./ERPTrans线下{month}月.mdb"
    attach2 = f"./{year2}{diff_month}/线下{year2}{month}01.zip"
    attaches = [attach1,attach2]
    # receiver = ["pengyaxiong@saselomo.com"]
    # cc = ["95468419@qq.com"]
    receiver = ["lilijuan@saselomo.com","dengqiuling@saselomo.com"]
    cc = ["chenwenrong@saselomo.com"]
    # m.mail(contents,subject,receiver,attaches,cc)
    
    print("完成!")

if __name__ == '__main__':
    main()
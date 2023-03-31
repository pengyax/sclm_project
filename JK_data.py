import pandas as pd
from sql_engine import connect
import numpy as np
import os
import save_table_to_access as sv
import mail_finance as m
import zipfile

conn_jk = connect('JK')

def format_string(df):
  for i in df.dtypes.to_dict().items():
    if i[1] == np.dtype('O'):
      try:
        df[i[0]] = df[i[0]].apply(lambda x: x.encode('latin1').decode('GBK') if x != None else None )
      except UnicodeEncodeError:
        df[i[0]]
  return df

def jk(type,year,month,diff_month):
  if type == '库存':
    file_name = f"./{year}{diff_month}/健康系统商品库存表{year}{diff_month}01.xlsx"
    sql = f"""  SELECT DISTINCT CompanyName 公司名称,WareHouseId 仓库,PlatformName 品牌,GoodsNo 商品编号 ,GoodsName 商品名称,OutGoingStock 仓库实际库存
    ,DistributableStock 销售可分配库存,AllocatedInventory 销售可用库存
    ,DisOutgoingStock 次品库存 FROM  [dbo].[DH_WareStockSnapshot] WHERE CreateTime>'{year}-{diff_month}-01'AND CreateTime<'{year}-{diff_month}-02' 
    AND (OutGoingStock>0 OR  DisOutgoingStock>0)""" 
  
  elif type == '未发货':
    file_name = f"./{year}{diff_month}/健康系统未发货报表{year}{diff_month}01.xlsx"
    sql = f"""SELECT * FROM [dbo].[DH_OrderNoShipMonthBak] WITH(NOLOCK) WHERE BakTime>'{year}-{diff_month}-01'  """ 
    
  elif type == '余额':
    file_name = f"./{year}{diff_month}/健康系统余额{year}{diff_month}01.xlsx"
    sql = f"""SELECT AgentWechat AS CustomerId, AgentName AS CustomerName, TB.SellerName AS CustomerClass, 
    TB.ActivityId AS activityID, ActivityName AS activityName, Balance AS PreDeposit,AC.ActivityCWTypeName AS actcategory,baktime
    FROM DH_TopUserBalance_Dail_Bak TB
    LEFT JOIN SCLMERPDB_UE.dbo.DH_Activity AC ON TB.ActivityId= AC.F_Id WHERE BakTime >'{year}-{diff_month}-01' AND BakTime<'{year}-{diff_month}-02' and 
    (AgentWechat not LIKE'%测试%' and agentwechat not like'%test%' and agentname not LIKE'%测试%' and agentname not like'%test%' 
    and agentname not like '%测试程%' and agentname not like '%黄明亮%') """ 
    
  elif type == '回款access':
    file_name = f"./{year}{diff_month}/胶原系统回款报表Access{year}{diff_month}01.xlsx"
    sql = f""" SELECT '余额' AS 'Account',(CASE InOutType WHEN 1 THEN '收' ELSE '支' END)AS Type,
ISNULL(SourceNo,'') AS TradeID,AgentName AS Customer,AC.OldId AS projectid, AC.ActivityName,
(CASE WHEN ActivityTypeNo=5 THEN '002' ELSE (CASE ActivityCWTypeNo WHEN 1 THEN '001' WHEN 2 THEN '002' ELSE '000' END) END) AS procatno,BA.platformName AS Shop,Item AS Cause,
(CASE InOutType WHEN 1  THEN InValue ELSE OutValue END ) AS MONEY,BA.platformName AS brand
,AgentWechat AS CustomerID, ISNULL(AudiTime,ApplyTime) AS AuditTime
,ISNULL((CASE  WHEN  ISNULL(PaymentAccount,'')<>'' THEN PaymentAccount ELSE PayAccount END) ,'') AS 'PayAcccount'
,ISNULL(CardNo,'') IDCardNo
FROM DH_BalanceApply AS BA
INNER JOIN [dbo].[DH_Activity] AC ON BA.ActivityId=AC.F_Id  AND IsDeleted=0  AND AC.ActivityCWTypeNo IN(1,2)
LEFT JOIN DH_UserInfo UI ON UI.SellerNo = BA.PlatformNo AND UI.ID = BA.AgentId
WHERE Status=1  AND BA.Deleted=0
AND (
 Item IN('货物预存款','政策余额充值','保证金收支','代理清算','三合一转款','全品牌转出','全品牌转入')
 OR (Item='政策余额转款' AND AC.ActivityCWTypeNo IN(1,2)) -- 政策余额转款 需要保证金 和 现金
 OR SourceNo IN(SELECT OrderNo FROM DH_BalanceTrans BT -- 客户余额互转 现金转保证金
    INNER JOIN [DH_Activity] OAC ON BT.OutActivityId = OAC.F_Id AND OAC.ActivityCWTypeNo=1
    INNER JOIN [DH_Activity] IAC ON BT.InActivityId = IAC.F_Id AND IAC.ActivityCWTypeNo=2
    WHERE ISNULL(BT.TransType,0)=0 AND Status=1 AND Deleted=0
   )
 OR SourceNo IN(SELECT OrderNo FROM DH_BalanceTrans BT -- 客户余额互转 保证金转现金
    INNER JOIN [DH_Activity] OAC ON BT.OutActivityId = OAC.F_Id AND OAC.ActivityCWTypeNo=2
    INNER JOIN [DH_Activity] IAC ON BT.InActivityId = IAC.F_Id AND IAC.ActivityCWTypeNo=1
    WHERE ISNULL(BT.TransType,0)=0 AND Status=1 AND Deleted=0
   )
)
AND AudiTime >='{year}-{month}-01' AND AudiTime<'{year}-{diff_month}-01'
ORDER BY AudiTime
"""

  elif type == '回款':
    file_name = f"./{year}{diff_month}/胶原系统回款报表{year}{diff_month}01.xlsx"
    sql = f""" SELECT
	'余额' AS 'Account',
	ISNULL( ( CASE WHEN ISNULL( PaymentAccount, '' ) <> '' THEN PaymentAccount ELSE PayAccount END ), '' ) AS 'PayAcccount',
	( CASE InOutType WHEN 1 THEN '收' ELSE '支' END ) AS Type,
	ISNULL( SourceNo, '' ) AS TradeID,
	AgentName AS Customer,
	AC.OldId AS projectid,
	AC.ActivityName,
	( CASE WHEN ActivityTypeNo = 5 THEN '002' ELSE ( CASE ActivityCWTypeNo WHEN 1 THEN '001' WHEN 2 THEN '002' ELSE '000' END ) END ) AS procatno,
	BA.platformName AS Shop,
	Item AS Cause,
	BA.platformName AS brand,
	( CASE InOutType WHEN 1 THEN InValue ELSE OutValue END ) AS MONEY,
	AgentWechat AS CustomerID,
	ISNULL( AudiTime, ApplyTime ) AS AuditTime,
	ISNULL( CardNo, '' ) IDCardNo 
FROM
	DH_BalanceApply AS BA
	INNER JOIN [dbo].[DH_Activity] AC ON BA.ActivityId= AC.F_Id 
	AND IsDeleted = 0 
-- 	AND AC.ActivityCWTypeNo IN ( 1, 2 )
	LEFT JOIN DH_UserInfo UI ON UI.SellerNo = BA.PlatformNo 
	AND UI.ID = BA.AgentId 
WHERE
	Status = 1 
	AND BA.Deleted= 0 
-- 	and item in ()
	AND AudiTime >= '{year}-{month}-01' 
	AND AudiTime < '{year}-{diff_month}-01' 
ORDER BY
	AudiTime
"""

  elif type == '出库':
    file_name = f"./{year}{diff_month}/胶原系统出库报表{year}{diff_month}01.xlsx"
    sql = f"""
        SELECT
    '商品销售' AS Reason,
    '出' AS InorOut,
    '10' AS WarehouseNO,
    d.orderNo,
    a.agentWechat CustomerID,
    a.AgentName CustomerName,
    '胶原蛋白' shop,
    '胶原蛋白' brand,
    d.goodsNo Goodsno,
    d.goodsTitle Goodsname,
    d.spec,
    d.totalCount,
    d.totalAmount total,
    AC.OldId AS projectid,
    ac.ActivityName,
    (CASE WHEN ActivityTypeNo=5 THEN '002' ELSE (CASE ActivityCWTypeNo WHEN 1 THEN '001' WHEN 2 THEN '002' ELSE '000' END) END) AS procatno,
    a.SendTime
    FROM
      DH_Order_Shipment a
      LEFT JOIN dbo.DH_Order_Shipments_Relation b ON a.shipNo= b.shipNo
      LEFT JOIN dbo.DH_Order c ON a.orderNo= c.orderNo 
      LEFT JOIN dbo.DH_Order_Detail d ON c.orderNo= d.orderNo AND b.orderDetailNo= d.orderDetailNo
      INNER JOIN [dbo].[DH_Activity] AC ON c.ActivityId=AC.F_Id  AND ac.IsDeleted=0
    WHERE
      1=1
      and a.sendTime >= '{year}-{month}-01'
      and a.sendTime < '{year}-{diff_month}-01'
      order by a.sendTime
        """

  elif type == '出库access':
    file_name = f"./{year}{diff_month}/胶原系统出库报表Access{year}{diff_month}01.xlsx"
    sql = f"""
        SELECT
    '商品销售' AS Reason,
    '出' AS InorOut,
    '10' AS WarehouseNO,
    d.orderNo,
    a.agentWechat CustomerID,
    AC.OldId AS projectid,
    (CASE WHEN ActivityTypeNo=5 THEN '002' ELSE (CASE ActivityCWTypeNo WHEN 1 THEN '001' WHEN 2 THEN '002' ELSE '000' END) END) AS procatno,
    '胶原蛋白' shop,
    d.goodsNo Goodsno,
    d.goodsTitle Goodsname,
    d.spec,
    d.totalCount,
    d.totalAmount total,
    '胶原蛋白' brand,
    a.SendTime
    FROM
      DH_Order_Shipment a
      LEFT JOIN dbo.DH_Order_Shipments_Relation b ON a.shipNo= b.shipNo
      LEFT JOIN dbo.DH_Order c ON a.orderNo= c.orderNo 
      LEFT JOIN dbo.DH_Order_Detail d ON c.orderNo= d.orderNo AND b.orderDetailNo= d.orderDetailNo
      INNER JOIN [dbo].[DH_Activity] AC ON c.ActivityId=AC.F_Id  AND ac.IsDeleted=0
      WHERE
      1=1
      and a.sendTime >= '{year}-{month}-01'
      and a.sendTime < '{year}-{diff_month}-01'
      order by a.sendTime
        """
    
  df = pd.read_sql(sql,conn_jk)
  df = format_string(df)
  df.to_excel(file_name,index = False)
  return df
 
def zip_files(files, zip_name):
    zip = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
    for file in files:
        print('compressing', file)
        zip.write(file)
    zip.close()
    print ('compressing finished')
    
def main():
    year = 2021
    month = 5
    diff_month = ("00" + str(6) )[-2:]
    jk("库存",year,month,diff_month)
    jk("未发货",year,month,diff_month)
    jk("余额",year,month,diff_month)
    df_access_Account_in_out = jk("回款access",year,month,diff_month)
    jk("回款",year,month,diff_month)
    jk("出库",year,month,diff_month)
    df_access_sales = jk("出库access",year,month,diff_month)
    
    f1 = f"./{year}{diff_month}/健康系统商品库存表{year}{diff_month}01.xlsx"
    f2 = f"./{year}{diff_month}/健康系统未发货报表{year}{diff_month}01.xlsx"
    f3 = f"./{year}{diff_month}/健康系统余额{year}{diff_month}01.xlsx"
    f4 = f"./{year}{diff_month}/胶原系统回款报表{year}{diff_month}01.xlsx"
    files = [f1,f2,f3,f4]
    zip_file = f'./{year}{diff_month}/健康{year}{month}月.zip'
    zip_files(files,zip_file)
    
    mdb_name = f'ERPTrans{year}{month}月胶原'
    sv.save_table_to_access_db(
        df_access_Account_in_out, mdb_name, 'Account_in_out')
    sv.save_table_to_access_db(df_access_sales, mdb_name, 'Sales')
    
    contents = ["Hi",
                f"请查收{month}月健康数据。"]
    subject = f"{month}月线下数据"
    # 发送邮件 yag.send("收件人",邮件标题,邮件内容)
    attach1 = f"./ERPTrans{year}{month}月胶原.mdb"
    attach2 = f'./{year}{diff_month}/健康{year}{month}月.zip'
    attaches = [attach1,attach2]
    # receiver = ["pengyaxiong@saselomo.com"]
    # cc = ["95468419@qq.com"]
    receiver = ["lilijuan@saselomo.com","jinmeimin@saselomo.com"]
    cc = ["chenwenrong@saselomo.com"]
    # m.mail(contents,subject,receiver,attaches,cc)

    print("完成！")

if __name__=='__main__':
    main()
    
    
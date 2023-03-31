import pandas as pd
from sql_engine import connect
import numpy as np
import os
import zipfile
import mail_finance as m


conn_field = connect('field')

def format_string(df):
  for i in df.dtypes.to_dict().items():
    if i[1] == np.dtype('O'):
      try:
        df[i[0]] = df[i[0]].apply(lambda x: x.encode('latin1').decode('GBK') if x != None else None )
      except UnicodeEncodeError:
        df[i[0]]
  return df

def field(type,year1,year2,month,diff_month):
  if type == '库存':
    file_name = f"./{year2}{diff_month}/田野系统商品库存表{year2}{diff_month}01.xlsx"
    sql = f""" SELECT DISTINCT CreateTime,CompanyName 公司名称,PlatformName 品牌,GoodsNo 商品编号 ,GoodsName 商品名称,OutGoingStock 仓库实际库存
    ,DistributableStock 销售可分配库存,AllocatedInventory 销售可用库存
    ,DisOutgoingStock 次品库存,OutGoingStock-DistributableStock-AllocatedInventory FROM  [dbo].[DH_WareStockSnapshot] WHERE CreateTime>'{year1}-{diff_month}-01'AND CreateTime<'{year2}-{diff_month}-02' 
    AND (OutGoingStock>0 OR  DisOutgoingStock>0)""" 
  
  elif type == '售后':
    file_name = f"./{year2}{diff_month}/田野系统售后表{year2}{diff_month}01.xlsx"
    sql = f"""SELECT CASE
    WHEN a.platformOrderNo = 'RETURN' THEN
    '销售退货'
    WHEN a.platformOrderNo = 'EXCHANGE'THEN
    '销售换货'
          ELSE
              '其他'
        END AS Reason,
        '入' AS InorOut,
        '11' AS WarehouseNO,
        ('''' + a.OldOrderNo) AS TradeNo,
      ('''' + o.orderNo) AS orderNo,
        '田野社交' AS shop,
        '田野社交' AS brand,
        b.goodsNo AS Goodsno,
        b.goodsTitle AS Goodsname,
      g.GoodsClassName_CP,
        b.refundCount  AS count,
      d.averagePrice AS price,
      b.refundCount * d.averagePrice AS TotalAmount,
    a.orderStatus AS '是否一件代发',
        a.paytime AS AuditTime
    FROM dbo.Tanke_AfterSale a
      INNER JOIN dbo.Tanke_AfterSale_Details b
          ON a.AfterNo = b.AfterNo
    INNER JOIN DH_GOODSSPU g ON b.goodsNo = g.GoodsNo
      INNER JOIN dbo.Tanke_Order o ON a.OldOrderNo=o.platformOrderNo
      INNER JOIN dbo.TanKe_Order_Detail d ON d.orderNo=o.orderNo
    WHERE a.refundFlag in (88,99) AND b.goodsNo=d.goodsNo
        AND
        (
            a.paytime > '{year1}-{month}-01 00:00:00'
            AND a.paytime < '{year2}-{diff_month}-01 00:00:00'
        )
    ORDER BY a.paytime DESC """ 
    
  elif type == '出库':
    file_name = f"./{year2}{diff_month}/田野系统销售出库表{year2}{diff_month}01.xlsx"
    sql = f"""SELECT 
    '商品销售' AS Reason,
    '出' AS InorOut,
    '10' AS WarehouseNO,
    a.orderNo,
    a.platformOrderNo,
    a.supplier_name,
    a.supplier_code,
    a.WareRoomName,
    a.WareRoomCode,
    (case when a.saleType=1 then '一件代发' else '自营' end)saleType  ,
    b.goodsNo,
    b.goodsTitle,
    g.GoodsClassName_CP,
    b.totalCount,
    b.totalAmount,
    isnull(b.ActDiscount,0) 活动优惠,
    isnull(b.DisAmount,0) 优惠卷优惠,
    b.cost_price ,
    a.sendTime,
    a.postcode
    FROM dbo.Tanke_Order AS a 
    INNER JOIN dbo.TanKe_Order_Detail AS b ON b.orderNo = a.orderNo 
    INNER JOIN DH_GOODSSPU g ON b.goodsNo = g.GoodsNo
    WHERE a.orderStatus=308 AND a.isDeleted=0 
    AND ((a.sendTime>='{year1}-{month}-01 00:00:00.000' AND  a.sendTime<'{year2}-{diff_month}-01 00:00:00.000')) """ 


  elif type == '邮费':
      file_name = f"./{year2}{diff_month}/田野系统销售出库订单邮费表{year2}{diff_month}01.xlsx"
      sql = f"""SELECT 
      '商品销售' AS Reason,
      '出' AS InorOut,
      '10' AS WarehouseNO,
      a.orderNo,
      a.platformOrderNo,
      a.supplier_name,
      a.supplier_code,
      a.WareRoomName,
      a.WareRoomCode,
      (case when a.saleType=1 then '一件代发' else '自营' end)saleType  ,
      a.totalAmount,
      a.realAmount,
      a.fee,
      isnull(a.Feediscount,0) 邮费优惠,
      a.sendTime,
      a.postcode
      FROM dbo.Tanke_Order AS a 
      WHERE a.orderStatus=308 AND a.isDeleted=0 
      AND ((a.sendTime>='{year1}-{month}-01 00:00:00.000' AND  a.sendTime<'{year2}-{diff_month}-01 00:00:00.000')) """ 

  df = pd.read_sql(sql,conn_field)
  df = format_string(df)
  df.to_excel(file_name,index = False)
  
def zip_files(zip_name,year,month):
    zip = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
    filelist = []
    for root, dirs, files in os.walk(f".\\{year}{month}", topdown=False):
        for name in files:
            file = os.path.join(root, name)
            filelist.append(file)
 
    for file in filelist:
        print('compressing', file)
        zip.write(file)
    zip.close()
    print ('compressing finished')


     
def main():
  year1 = 2021
  year2 = 2021
  month = 5
  diff_month = ("00" + str(6) )[-2:]
  field("库存",year1,year2,month,diff_month)
  field("售后",year1,year2,month,diff_month)
  field("出库",year1,year2,month,diff_month)
  field("邮费",year1,year2,month,diff_month)
  zip_files(f"./{year2}{diff_month}/田野{year2}{month}.zip",year2,diff_month)
  
  contents = ["Hi",
                f"请查收{month}月田野数据。"]
  subject = f"{month}月田野数据"
  # 发送邮件 yag.send("收件人",邮件标题,邮件内容)
  attach1 = f"./{year2}{diff_month}/田野{year2}{month}.zip"
  attaches = [attach1]
  # receiver = ["pengyaxiong@saselomo.com"]
  # cc = ["95468419@qq.com"]
  receiver = ["lilijuan@saselomo.com","wujing@saselomo.com"]
  cc = ["chenwenrong@saselomo.com"]
  m.mail(contents,subject,receiver,attaches,cc)
  
  print("完成！")

if __name__=='__main__':
    main()



import pandas as pd
from sql_engine import connect
import numpy as np
import zipfile
import mail_finance as m

conn_sclm = connect('sclm')

def format_string(df):
  for i in df.dtypes.to_dict().items():
    if i[1] == np.dtype('O'):
      try:
        df[i[0]] = df[i[0]].apply(lambda x: x.encode('latin1').decode('GBK') if x != None else None )
      except UnicodeEncodeError:
        df[i[0]]
  return df

def sclm(type,year,month,diff_month,conn = conn_sclm ):
  if type == '库存':
    file_name = f"./{year}{diff_month}/三草系统商品库存表{year}{diff_month}01.xlsx"
    sql = f"""  SELECT DISTINCT CompanyName 公司名称,WareHouseId 仓库,PlatformName 品牌,GoodsNo 商品编号 ,GoodsName 商品名称,OutGoingStock 仓库实际库存
    ,DistributableStock 销售可分配库存,AllocatedInventory 销售可用库存
    ,DisOutgoingStock 次品库存 FROM  [dbo].[DH_WareStockSnapshot] WHERE CreateTime>'{year}-{diff_month}-01'AND CreateTime<'{year}-{diff_month}-02' 
    AND (OutGoingStock>0 OR  DisOutgoingStock>0)""" 
  
  elif type == '未发货':
    file_name = f"./{year}{diff_month}/三草系统未发货报表{year}{diff_month}01.xlsx"
    sql = f"""SELECT * FROM [dbo].[DH_OrderNoShipMonthBak] WITH(NOLOCK) WHERE BakTime>'{year}-{diff_month}-01'  """ 
    
  elif type == '余额':
    file_name = f"./{year}{diff_month}/三草系统余额(去除全品牌和养面膜){year}{diff_month}01.xlsx"
    sql = f"""SELECT AgentWechat AS CustomerId, AgentName AS CustomerName, TB.SellerName AS CustomerClass, 
    TB.ActivityId AS activityID, ActivityName AS activityName,AC.ActivityCWTypeName AS actcategory, Balance AS PreDeposit,'' policy_id,baktime
    FROM DH_TopUserBalance_Bak_Daily201201 TB
    LEFT JOIN SCLMERPDB_UE.dbo.DH_Activity AC ON TB.ActivityId= AC.F_Id WHERE BakTime >'{year}-{diff_month}-01' AND BakTime<'{year}-{diff_month}-02' AND TB.SellerNo NOT IN ('800018','800003') and 
    (AgentWechat not LIKE'%测试%' and agentwechat not like'%test%' and agentname not LIKE'%测试%' and agentname not like'%test%' 
    and agentname not like '%测试程%' and agentname not like '%黄明亮%')  """ 
    conn = connect('sclm')
    
  elif type == '余额备份':
    file_name = f"./{year}{diff_month}/三草系统余额备份(全品牌和养面膜){year}{diff_month}01.xlsx"
    sql = f"""SELECT WeChat,RealName,SellerName,activityid,ActivityName,ActivityCWTypeName,cast(round(amt_total/100,2)  as  numeric(10,2)) balance,policy_id,CreateTime FROM A_SybBalanceStock WHERE CreateTime>'{year}-{diff_month}-01' AND CreateTime<'{year}-{diff_month}-02' AND (RealName not LIKE'%测试%' and RealName not like'%test%' and RealName not LIKE'%测试%' and RealName not like'%test%' 
    and RealName not like '%测试程%' and RealName not like '%黄明亮%')  """ 
    conn = connect('sclm_copy')
    
  df = pd.read_sql(sql,conn)
  df = format_string(df)
  df.to_excel(file_name,index = False)
  
def sclm_balance(file1,file2,target_file_path):
    df1 = pd.read_excel(file1)
    df2 = pd.read_excel(file2)
    df2.columns = ['WeChat','RealName','SellerName','activityid','ActivityName','ActivityCWTypeName','balance','policy_id','CreateTime']
    list = [df1,df2]
    pd.concat(list,ignore_index=True).to_excel(target_file_path,index=False)
  
  
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
    sclm("库存",year,month,diff_month)
    sclm("未发货",year,month,diff_month)
    sclm("余额",year,month,diff_month)
    sclm("余额备份",year,month,diff_month)
    
    sclm_balance_file1 = f"./{year}{diff_month}/三草系统余额备份(全品牌和养面膜){year}{diff_month}01.xlsx"
    sclm_balance_file2 = f"./{year}{diff_month}/三草系统余额(去除全品牌和养面膜){year}{diff_month}01.xlsx"
    sclm_balance_target_file = f"./{year}{diff_month}/三草系统余额{year}{diff_month}01.xlsx"
    sclm_balance(sclm_balance_file1,sclm_balance_file2,sclm_balance_target_file)
    
    
    f1 = f"./{year}{diff_month}/三草系统余额{year}{diff_month}01.xlsx"
    f2 = f"./{year}{diff_month}/三草系统余额备份(全品牌和养面膜){year}{diff_month}01.xlsx"
    f3 = f"./{year}{diff_month}/三草系统余额(去除全品牌和养面膜){year}{diff_month}01.xlsx"
    f4 = f"./{year}{diff_month}/三草系统商品库存表{year}{diff_month}01.xlsx"
    f5 = f"./{year}{diff_month}/三草系统未发货报表{year}{diff_month}01.xlsx"
    files = [f1,f2,f3,f4,f5]
    
    zip_file = f'./{year}{diff_month}/三草{year}{month}.zip'
    #压缩包名字
    zip_files(files, zip_file)

    contents = ["Hi",
                f"请查收{month}月三草数据。"]
    subject = f"{month}月三草数据"
    # 发送邮件 yag.send("收件人",邮件标题,邮件内容)
    attach1 = f'./{year}{diff_month}/三草{year}{month}.zip'
    attaches = [attach1]
    # receiver = ["pengyaxiong@saselomo.com"]
    # cc = ["95468419@qq.com"]
    receiver = ["lilijuan@saselomo.com"]
    cc = ["chenwenrong@saselomo.com"]
    m.mail(contents,subject,receiver,attaches,cc)
    
    
    print("完成！")

if __name__=='__main__':
    main()



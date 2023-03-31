import pandas as pd
from sql_engine import connect
import save_table_to_access as sv
import mail_finance as m

jky_con = connect('JKY')

year = '2021'
month = '05'
last_month = '04'


# ==============
# 拷贝初始化表
# ==============

sql_fomat = f'''
--已发货订单明细表   1.上月发货销售订单明细数据
select * into temp_jky{year}_{month}_ordermx_yfh from temp_jky{year}_{last_month}_ordermx_yfh
truncate table temp_jky{year}_{month}_ordermx_yfh


-- 售后发货明细账   2.上月售后发货明细数据
select * into temp_jky{year}_{month}_order_fh from temp_jky{year}_{last_month}_order_fh
truncate table temp_jky{year}_{month}_order_fh


 --******吉客云  售后退货明细账   3.上月售后退货明细数据   --数据异常

select * into temp_jky{year}_{month}_order_th from temp_jky{year}_{last_month}_order_th
truncate table temp_jky{year}_{month}_order_th


--******吉客云  售后补退款明细账   2月无   4.上月退款补偿明细
--select * FROM temp_jky{year}_{last_month}_order_btk

select * into temp_jky{year}_{month}_order_btk from temp_jky{year}_{last_month}_order_btk
truncate table temp_jky{year}_{month}_order_btk

--******吉客云错漏发货明细账      5.上月错漏补发明细数据

select * into temp_jky{year}_{month}_order_lf from temp_jky{year}_{last_month}_order_lf
truncate table temp_jky{year}_{month}_order_lf

---******吉客云 月初库存表   6.上月期初库存数据


select * into temp_jky{year}_{month}_stock_start from temp_jky{year}_{last_month}_stock_start
truncate table temp_jky{year}_{month}_stock_start

---******吉客云 月末库存表   7.上月期末库存数据

select * into temp_jky{year}_{month}_stock_end from temp_jky{year}_{last_month}_stock_end
truncate table temp_jky{year}_{month}_stock_end

 --8.	上月非销售出库明细数据

select * into temp_jky{year}_{month}_rk_all from temp_jky{year}_{last_month}_rk_all
truncate table temp_jky{year}_{month}_rk_all

--2月查1月已完成数据    --9.之前月份已发货未完成上月完成订单明细数据

select * into temp_jky{year}_{month}cha_{int(last_month)}_ordermx_yqs from temp_jky{year}_{last_month}cha_{int(last_month)-1}_ordermx_yqs
truncate table temp_jky{year}_{month}cha_{int(last_month)}_ordermx_yqs

--******吉客云截止到2月底发货在途明细      10.当前已发货未完成订单明细数据      数据异常 

select * into temp_jky{year}_{month}_ordermx_yfhwqs_all from temp_jky{year}_{last_month}_ordermx_yfhwqs_all
truncate table temp_jky{year}_{month}_ordermx_yfhwqs_all
'''

# ==============
# 创建辅助表
# ==============

sql_step_pre = f'''
SELECT * INTO temp_jky{year}_{month}_crk FROM temp_jky{year}_{month}_rk_all WHERE 出入库类型 NOT IN ('销售出库','销售退货');
 
select b.* INTO temp_jky{year}_{month}_ordermx_yfh_1qs FROM temp_jky{year}_{last_month}_ordermx_yfh a INNER JOIN temp_jky{year}_{month}cha_{int(last_month)}_ordermx_yqs b ON  a.订单编号=b.订单编号 AND a.货品编号=b.货品编号
WHERE a.订单状态='发货在途' AND b.订单状态='已完成';
  
SELECT * INTO temp_jky{year}_{month}_ordermx_yqs FROM (
select * FROM temp_jky{year}_{month}_ordermx_yfh_1qs
UNION 
select * FROM temp_jky{year}_{month}_ordermx_yfh WHERE 订单状态='已完成'
) a;

-- ==============
-- 创建Account_In_Out表
-- ==============

select '余额' AS account,'收' AS type,CASE when  网店订单号='' THEN 订单编号 ELSE  网店订单号 end  AS 外部平台单号 ,销售渠道 AS 店铺名称,188 AS  project,'001' AS procat,'电商' AS shop,'电商渠道支付' AS cause,分摊后金额,'电商渠道' AS brand,销售渠道 AS customerid,发货时间
INTO temp_jky{year}_{month}_Account_In_Out
FROM temp_jky{year}_{month}_ordermx_yqs WHERE  cast(分摊后金额 AS float)>0;

INSERT temp_jky{year}_{month}_Account_In_Out
select '余额' AS account,'支' AS type,CASE when  网店订单号='' THEN 订单编号 ELSE  网店订单号 end,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,'售后退款' AS cause,-1*CAST(分摊后金额 AS FLOAT),'电商渠道' AS brand,销售渠道 AS customerid,发货时间
FROM temp_jky{year}_{month}_ordermx_yqs WHERE  cast(分摊后金额 AS float)<0;

UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-XHS' WHERE customerid='三草两木小红书旗舰店';
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-YZ' WHERE customerid='三草两木有赞店铺';
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-JD' WHERE customerid='三草两木京东旗舰店';
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-TM' WHERE customerid='三草两木天猫旗舰店';
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-TMZM' WHERE customerid='Janemua珍慕天猫旗舰店' ;
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-JMYP' WHERE customerid='三草两木聚美优品';
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-DY' WHERE customerid='三草两木抖音旗舰店';
UPDATE temp_jky{year}_{month}_Account_In_Out SET customerid='SCLMDS-DYZM' WHERE customerid='Janemua珍慕抖音旗舰店';


-- ==============
-- 创建sales表
-- ==============

 SELECT '商品销售' AS reason,'出' AS inorout,'12' AS wareid, CASE WHEN (网店订单号 IS NULL OR 网店订单号='') THEN 订单编号 ELSE 网店订单号 end   网店订单号,销售渠道 ,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间,[货品-销售类型] AS '货品销售类型',货品备注,客服备注
INTO  temp_jky{year}_{month}_sales
 FROM temp_jky{year}_{month}_ordermx_yqs  WHERE   CAST(数量 AS FLOAT)>0;

 --加入退货数据  
  INSERT temp_jky{year}_{month}_sales
 SELECT  '销售退货','入','12', CASE WHEN 网店订单号 IS NULL THEN 订单编号 ELSE 网店订单号 end,销售渠道 ,188,'001', '电商' ,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' ,发货时间 ,[货品-销售类型] AS '货品销售类型',货品备注,客服备注
 FROM temp_jky{year}_{month}_ordermx_yqs  WHERE   CAST(数量 AS FLOAT)<0;


 -- 电商员工自建单中网店单号含有BF开头的单据需要手动处理替换掉
   select * FROM temp_jky{year}_{month}_sales WHERE 网店订单号 LIKE '%BF%';
   UPDATE temp_jky{year}_{month}_sales SET 网店订单号= REPLACE(网店订单号,'BF-','');
    UPDATE temp_jky{year}_{month}_sales SET 网店订单号= REPLACE(网店订单号,'BFF','') WHERE 网店订单号 LIKE '%BFF%';
   UPDATE temp_jky{year}_{month}_sales SET 网店订单号= REPLACE(网店订单号,'BF','') WHERE 网店订单号 LIKE '%BF%';


-- ==============
-- 创建stocks表
-- ==============

SELECT 出入库类型,'入' AS inorout,出入库原因,仓库,单据编号, 往来单位,货品编号, 货品名称,规格, 入库数量,入库金额,'三草两木' AS brand,  系统出入库时间 ,操作员,备注
INTO 	temp_jky{year}_{month}_stocks
FROM temp_jky{year}_{month}_crk  WHERE  出入库类型 IN ('采购入库','调拨入库','盘盈入库','其他入库') and   货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDS20043001','SCLMDS20043007','JMWSML170926001','SCLMDSSD200728001','SCLMTM200114004' );


INSERT	temp_jky{year}_{month}_stocks
SELECT 出入库类型,'出' AS inorout,出入库原因,仓库,单据编号, 往来单位,货品编号, 货品名称,规格, 出库数量,出库金额,'三草两木' AS brand,  系统出入库时间 ,操作员,备注
FROM temp_jky{year}_{month}_crk  WHERE  出入库类型 IN ('采购退货','调拨出库','盘亏出库','其他出库') and   货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMDSSD200728001','SCLMTM200114004');


UPDATE temp_jky{year}_{month}_stocks SET 往来单位='ZJCMHZ' WHERE 往来单位='浙江传美';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='SHTSMY' WHERE 往来单位='上海天实贸易有限公司';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='SHTSMY' WHERE 往来单位='上海天实实业集团有限公司';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='SHCMSY' WHERE 往来单位='传美实业';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='SHCMSY' WHERE 往来单位='上海传美实业有限公司';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='WHSCLMSMYXGS' WHERE 往来单位='武汉三草两木商贸有限公司';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='ZJCMHZ' WHERE 往来单位='传美化妆品（浙江自贸区）有限公司';
UPDATE temp_jky{year}_{month}_stocks SET 往来单位='GXJDGYLYXGS' WHERE 往来单位='广西简单供应链有限公司';


'''





# ==============
# 分流Account_In_Out表至3个渠道
# ==============

sql_step_Account_In_Out_sclm = f'''
SELECT
	account,
	type,
	[外部平台单号] TradeID,店铺名称 Customer,
	project,
	procat,
	shop,
	cause,
	cast([分摊后金额] as numeric(18,2)) MONEY,
	brand,
	customerid,发货时间 AuditTime,
	'' PayAcccount,
	'' IdNo 
FROM
	temp_jky{year}_{month}_Account_In_Out 
WHERE
	店铺名称 IN ( '三草两木抖音旗舰店', '三草两木京东旗舰店', '三草两木商贸网店', '三草两木天猫旗舰店', '三草两木小红书旗舰店' )
 '''

sql_step_Account_In_Out_shym = f'''
 SELECT *  from (
 select '余额' AS account,'收' AS type,网店订单号,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,'电商渠道支付' AS cause,分摊后金额,'电商渠道' AS brand,销售渠道 AS customerid,发货时间
  FROM temp_jky{year}_{month}_ordermx_yfh WHERE  cast(分摊后金额 AS float)>0 AND 销售渠道 IN ('三草两木有赞店铺','三草两木聚美优品')
UNION
   select '余额' AS account,'支' AS type,网店订单号,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,'售后退款' AS cause,-1*CAST(分摊后金额 AS FLOAT),'电商渠道' AS brand,销售渠道 AS customerid,发货时间
  FROM temp_jky{year}_{month}_ordermx_yfh WHERE  cast(分摊后金额 AS float)<0 AND 销售渠道 IN ('三草两木有赞店铺','三草两木聚美优品')
  ) a
'''

sql_step_Account_In_Out_sczm = f'''
 SELECT * FROM temp_jky{year}_{month}_Account_In_Out WHERE 店铺名称 IN ('Janemua珍慕天猫旗舰店','Janemua珍慕有赞旗舰店','Janemua珍慕抖音旗舰店','')
'''

# ==============
# 分流sales表至3个渠道
# ==============

sql_step_sales_sclm = f'''
 SELECT * FROM temp_jky{year}_{month}_sales WHERE 销售渠道  IN ( '三草两木抖音旗舰店','三草两木京东旗舰店','三草两木商贸网店','三草两木天猫旗舰店','三草两木小红书旗舰店')  and 数量 is not null
 AND 货品编号 NOT IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

# 当月明细

sql_step_sales_sclm_detail = f'''
SELECT '商品销售' AS reason,'出' AS inorout,发货仓库,网店订单号,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间 ,[货品-销售类型] AS '货品销售类型',货品备注
 FROM temp_jky{year}_{month}_ordermx_yfh  WHERE 销售渠道 IN ( '三草两木抖音旗舰店','三草两木京东旗舰店','三草两木商贸网店','三草两木天猫旗舰店','三草两木小红书旗舰店')   AND 货品编号 NOT IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''


sql_step_sales_shym = f'''
SELECT '商品销售' AS reason,'出' AS inorout,发货仓库,网店订单号,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间 ,[货品-销售类型] AS '货品销售类型',货品备注
 FROM temp_jky{year}_{month}_ordermx_yfh  WHERE 销售渠道 IN ('三草两木有赞店铺','三草两木聚美优品')   AND 货品编号 NOT IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''



sql_step_sales_sczm = f'''
 SELECT * FROM temp_jky{year}_{month}_sales WHERE 销售渠道 IN ('Janemua珍慕天猫旗舰店','Janemua珍慕有赞旗舰店','Janemua珍慕抖音旗舰店') and 数量 is not null
 AND 货品编号 NOT IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

# 当月明细

sql_step_sales_sczm_detail = f'''
SELECT '商品销售' AS reason,'出' AS inorout,发货仓库,网店订单号,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间 ,[货品-销售类型] AS '货品销售类型',货品备注
 FROM temp_jky{year}_{month}_ordermx_yfh  WHERE 销售渠道 IN ('Janemua珍慕天猫旗舰店','Janemua珍慕有赞旗舰店','Janemua珍慕抖音旗舰店')   AND 货品编号 NOT IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

# 刷单数据

sql_shuadan_sclm =f'''
  select '商品销售' AS reason,'出' AS inorout,发货仓库 AS wareid,网店订单号,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间
  --INTO #temp_jky{year}_{month}_sd
    FROM temp_jky{year}_{month}_ordermx_yfh WHERE 货品编号='SCLMDSSD200728001'
'''



# ==============
# 分流stocks表至3个渠道
# ==============

sql_step_stocks_sclm = f'''
-- 三草商贸
SELECT * FROM temp_jky{year}_{month}_stocks WHERE 仓库 NOT IN	('遇美有赞仓','珍慕彩妆仓')  AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_step_stocks_shym = f'''
-- 遇美
SELECT * FROM temp_jky{year}_{month}_stocks WHERE 仓库='遇美有赞仓' AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')

'''

sql_step_stocks_sczm = f'''
-- 珍幕
SELECT * FROM temp_jky{year}_{month}_stocks WHERE 仓库='珍慕彩妆仓' AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_stock_start_sclm = f'''
-- 三草商贸
 SELECT 仓库,货品编号,分类,	货品名称,库存量 FROM dbo.temp_jky{year}_{month}_stock_start WHERE 仓库 NOT IN	('遇美有赞仓','珍慕彩妆仓')   AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_stock_start_shym = f'''
-- 遇美
  SELECT 仓库,货品编号,分类,	货品名称,库存量 FROM dbo.temp_jky{year}_{month}_stock_start WHERE 仓库='遇美有赞仓'     AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_stock_start_sczm = f'''
-- 珍幕
  SELECT 仓库,货品编号,分类,	货品名称,库存量 FROM dbo.temp_jky{year}_{month}_stock_start WHERE 仓库='珍慕彩妆仓'     AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''


sql_stock_end_sclm = f'''
-- 三草商贸
 SELECT 仓库,货品编号,分类,	货品名称,库存量 FROM dbo.temp_jky{year}_{month}_stock_end WHERE 仓库 NOT IN	('遇美有赞仓','珍慕彩妆仓')       AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_stock_end_shym = f'''
-- 遇美
  SELECT 仓库,货品编号,分类,	货品名称,库存量 FROM dbo.temp_jky{year}_{month}_stock_end WHERE 仓库='遇美有赞仓'     AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_stock_end_sczm = f'''
-- 珍幕
  SELECT 仓库,货品编号,分类,	货品名称,库存量 FROM dbo.temp_jky{year}_{month}_stock_end WHERE 仓库='珍慕彩妆仓'     AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004')
'''

sql_ship_unfinish_sclm = f'''
-- 三草商贸
 SELECT '商品销售' AS reason,'出' AS inorout, 发货仓库, CASE	WHEN 网店订单号='' THEN 订单编号 ELSE 网店订单号 END AS 外部平台单号 ,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间,''AS  rctime,订单状态,[货品-销售类型] AS '货品销售类型',货品备注
  FROM temp_jky{year}_{month}_ordermx_yfhwqs_all  
   WHERE     销售渠道   IN ( '三草两木抖音旗舰店','三草两木京东旗舰店','三草两木商贸网店','三草两木天猫旗舰店','三草两木小红书旗舰店')
   AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004') 
-- 加上 SCLMTM200114004 商品信息筛选

-- 遇美  无 平台不会回传已完成状态
'''

sql_ship_unfinish_sczm = f'''
-- 珍幕
 SELECT '商品销售' AS reason,'出' AS inorout, 发货仓库, CASE	WHEN 网店订单号='' THEN 订单编号 ELSE 网店订单号 END AS 外部平台单号 ,销售渠道,188 AS project,'001' AS procat,'电商' AS shop,货品编号,货品名称,规格,数量,分摊后金额,'三草两木' AS brand,发货时间,''AS  rctime,订单状态,[货品-销售类型] AS '货品销售类型',货品备注
  FROM temp_jky{year}_{month}_ordermx_yfhwqs_all  
   WHERE     销售渠道 IN ('Janemua珍慕天猫旗舰店','Janemua珍慕有赞旗舰店','Janemua珍慕抖音旗舰店','')
   AND 货品编号 not IN  ('SCLMTM200114002','SCLMTM200114003','SCLMDSSD200728001','SCLMDS20043007','JMWSML170926001','SCLMDS20043001','SCLMTM200114004') 
'''



def save_mdb(seller):
  if seller == '三草':
    df_Account_In_Out_sclm = pd.read_sql(sql_step_Account_In_Out_sclm,jky_con)
    df_sales_sclm = pd.read_sql(sql_step_sales_sclm,jky_con)
    df_sales_sclm_detail = pd.read_sql(sql_step_sales_sclm_detail,jky_con)
    df_shuadan_sclm = pd.read_sql(sql_shuadan_sclm,jky_con)
    df_stocks_sclm = pd.read_sql(sql_step_stocks_sclm,jky_con)
    df_stock_start_sclm = pd.read_sql(sql_stock_start_sclm,jky_con)
    df_stock_end_sclm = pd.read_sql(sql_stock_end_sclm,jky_con)
    df_ship_unfinish_sclm = pd.read_sql(sql_ship_unfinish_sclm,jky_con)
    mdb_name = f'三草商贸(电商{year}.{month}月)'
    sv.save_table_to_access_db(
        df_Account_In_Out_sclm, mdb_name, 'Account_in_out')
    sv.save_table_to_access_db(df_sales_sclm, mdb_name, 'Sales')
    sv.save_table_to_access_db(df_sales_sclm_detail, mdb_name, f'发货订单{month}月明细')
    sv.save_table_to_access_db(df_shuadan_sclm, mdb_name, '刷单订单明细')
    sv.save_table_to_access_db(df_stocks_sclm, mdb_name, 'Stocks')
    sv.save_table_to_access_db(df_stock_start_sclm, mdb_name, '期初库存')
    sv.save_table_to_access_db(df_stock_end_sclm, mdb_name, '期末库存')
    sv.save_table_to_access_db(df_ship_unfinish_sclm, mdb_name, '已发货未签收订单明细')

  elif seller == '遇美':
    df_Account_In_Out_shym = pd.read_sql(sql_step_Account_In_Out_shym,jky_con)
    df_sales_shym = pd.read_sql(sql_step_sales_shym,jky_con)
    df_stocks_shym = pd.read_sql(sql_step_stocks_shym,jky_con)
    df_stock_start_shym = pd.read_sql(sql_stock_start_shym,jky_con)
    df_stock_end_shym = pd.read_sql(sql_stock_end_shym,jky_con)
    mdb_name = f'上海遇美(电商{year}.{month}月)'
    sv.save_table_to_access_db(
        df_Account_In_Out_shym, mdb_name, 'Account_in_out')
    sv.save_table_to_access_db(df_sales_shym, mdb_name, 'Sales')
    sv.save_table_to_access_db(df_stocks_shym, mdb_name, 'Stocks')
    sv.save_table_to_access_db(df_stock_start_shym, mdb_name, '期初库存')
    sv.save_table_to_access_db(df_stock_end_shym, mdb_name, '期末库存')
  
  elif seller == '珍慕':
    df_Account_In_Out_sczm = pd.read_sql(sql_step_Account_In_Out_sczm,jky_con)
    df_sales_sczm = pd.read_sql(sql_step_sales_sczm,jky_con)
    df_sales_sczm_detail = pd.read_sql(sql_step_sales_sczm_detail,jky_con)
    df_stocks_sczm = pd.read_sql(sql_step_stocks_sczm,jky_con)
    df_stock_start_sczm = pd.read_sql(sql_stock_start_sczm,jky_con)
    df_stock_end_sczm = pd.read_sql(sql_stock_end_sczm,jky_con)
    df_ship_unfinish_sczm = pd.read_sql(sql_ship_unfinish_sczm,jky_con)
    mdb_name = f'三草珍慕(电商{year}.{month}月)'
    sv.save_table_to_access_db(
        df_Account_In_Out_sczm, mdb_name, 'Account_in_out')
    sv.save_table_to_access_db(df_sales_sczm, mdb_name, 'Sales')
    sv.save_table_to_access_db(df_sales_sczm_detail, mdb_name, f'发货订单{month}月明细')
    sv.save_table_to_access_db(df_stocks_sczm, mdb_name, 'Stocks')
    sv.save_table_to_access_db(df_stock_start_sczm, mdb_name, '期初库存')
    sv.save_table_to_access_db(df_stock_end_sczm, mdb_name, '期末库存')
    sv.save_table_to_access_db(df_ship_unfinish_sczm, mdb_name, '已发货未签收订单明细')


def main():
  save_mdb('三草')
  save_mdb('遇美')
  save_mdb('珍慕')
  
if __name__ == '__main__':
  main()
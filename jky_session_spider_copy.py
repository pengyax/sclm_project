# coding:utf-8
from requests import Session
from requests import utils
import time
import hashlib
from urllib import parse
import time
import pandas as pd
import requests
import math
import datetime
import calendar
import json
from sql_engine import connect
from functools import reduce
import io
import urllib3
urllib3.disable_warnings()
from dic_rename import rename_format


class Jky(object):
    def __init__(self):
        self.session = Session()
        utils.add_dict_to_cookiejar(
            self.session.cookies, {
                '_ati': '1507160404051',
                'ati': '1507160404051',
                'company':'ä¸\x89è\x8d\x89ä¸¤æ\x9c¨é\x9b\x86å\x9b¢æ\x80»é\x83¨',
                'computer_id': '_9F3XQYJ6TK5G7LCVKLM',
                'forceChangePassword': '0',
                'member_id': '960025061393039872',
                'member_name': '647102',
                'system_ip': 'http://47.92.44.184',
                'user_id': '961498471830717056',
                'user_name': '2016097',
                'webSocket_ip': 'wss://ws.jackyun.com/ws'
            })
        self.secret = 'xKPUbOu7H9Xpm16cpRw06LNkPggpq8hI'

    def login(self):
        t = int(time.time()) * 1000
        query = (
            f'memberName=647102&userId=961498471830717056&password={self.ha("961498471830717056", "rr412412", t)}&timestamp={t}&nodeId=P6L5SO1ZA7V8BCYYEKI_&clientSn=GTSXIBIJ-LUIX-BROSWER&ati=1507160404051&loginDevice=web_browser'
        ).encode().decode('latin1')
        r = self.session.post('https://www.jky888.com/auth/v5/web/login',
            data=query,
            headers={
                'Host': 'www.jky888.com',
                'Connection': 'keep-alive',
                'Content-Length': '179',
                'Accept': '*/*',
                'clientId': 'jackyun_web_browser',
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
                'Content-Type':
                'application/x-www-form-urlencoded; charset=UTF-8',
                'Origin': 'https://www.jky888.com',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://www.jky888.com/login/login_web.html',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9'
            },
            verify=False)
        r = r.json()
        self.session.cookies.update({
            "token":
            r.get('result').get('data').get('token').get('access_token'),
            "refresh_token":
            r.get('result').get('data').get('token').get('refresh_token')
        })
        return

    def get_stock_curr(self):
        headers = {
            'Host': 'www.jky888.com',
            'Connection': 'keep-alive',
            'Content-Length': '1355',
            'Accept': 'text/plain, */*; q=0.01',
            'ati': f'{self.session.cookies.get("ati")}',
            'X-Requested-With': 'XMLHttpRequest',
            'Authorization': f'Bearer {self.session.cookies.get("token")}',
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.jky888.com',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer':
            'https://www.jky888.com/erp/stock/branch_stock_main.html?mode=sku&_t=337060&_winid=w6494',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }
        df = pd.DataFrame()
        for pageno in [0, 1]:
            query = {
                'timestamp':
                f'{int(time.time() * 1000)}',
                'access_token':
                f'Bearer {self.session.cookies.get("token")}',
                'appkey':
                'jackyun_web_browser',
                'sign':
                '4780BB1E4FE273EBEBB4B898202CEFCF',
                'serviceType':
                'stock.search',
                'searchType':
                '2',
                'searchWarehouseType':
                '1',
                'isHideZeroQuantity':
                '0',
                'isShowBlockup':
                '0',
                'warehouseId':
                '968606409259025152,965712961844183936,1025243866625639296,1017292810616734592,978065461043069312,970814096235365888,967961503880184448,960824757136982528',
                'pageIndex':
                f'{pageno}',
                'pageSize':
                '1000',
                'cols':
                '["goodsNo","goodsName","skuName","brandName","cateName","abcCate","skuBarcode","unitName","commonUnitName","assistUnit","goodsAssistUnit","warehouseName","warehouseCompanyName","warehouseDepartName","currentQuantity","canUseQuantity","minValue","maxValue","defaultVendName","defaultVendPrice","lastStockInTime","lastStockOutTime","distrubuteQuantity","lockingQuantity","purchasingQuantity","allocateQuantity","purchOrderPlanQuantity","salesReturnQuantity","productingQuantity","productWaitQuantity","defectiveQuanity","totalQuantity","retailPrice","costPrice","lastPurchPrice","costValue","yesterdayQuantity","weekQuantity","threedayQuantity","flagData","goodsField1","goodsField2"]'
            }
            query = self.query_to_format(query, 'stock')
            r = self.session.post(
                "https://www.jky888.com/jkyun/erp-stock/search/pagelist1",
                data=query,
                verify=False,
                headers=headers)
            r = pd.DataFrame(r.json().get('result').get('data'))
            for i in [
                    'lastInStockTime', 'lastOutStockTime', 'lastPurchTime',
                    'staDay', 'lastStockInTime', 'lastStockOutTime'
            ]:
                r[i] = r[i].fillna(0).apply(lambda x: time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(x / 1000))
                                            if x > 0 else None)
            r.drop('warehouseIds', axis=1, inplace=True)
            df = pd.concat([df, r])
        df['updateTime'] = datetime.datetime.today().strftime(
                '%Y-%m-%d %H:%M:%S')
        df.to_sql("JKY_goods_stock",
                 connect('bidata'),
                 index=None,
                 if_exists='append',
                 method='multi',
                 chunksize=100
                 )
        return

    def get_goods_data(self, pageSize=1000):
        headers = {
            'Host': 'www.jky888.com',
            'Connection': 'keep-alive',
            'Accept': 'text/plain, */*; q=0.01',
            'ati': f'{self.session.cookies.get("ati")}',
            'X-Requested-With': 'XMLHttpRequest',
            'Authorization': f'Bearer%20{self.session.cookies.get("token")}',
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer':
            'https://www.jky888.com/erp/goods_file_erp/goods_managet.html?_t=170675&_winid=w9166',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }
        query = {
            'timestamp':
            f'{int(time.time() * 1000)}',
            'access_token':
            f'Bearer {self.session.cookies.get("token")}',
            'appkey':
            'jackyun_web_browser',
            'blockUp':
            '0',
            'packageGood':
            '0',
            'pageIndex':
            f'{1 if pageSize <= 100 else 0}',
            'pageSize':
            f'{pageSize}',
            'cols':
            '["goodsNo","goodsName","cateName","abcCate","brandName","unitName","assistUnit","isBatchManagement","isSerailManagement","ownerName","flagData","orderIndex","gmtCreate","gmtModified","creator","modifiedBy","auditor","goodsField1","goodsField2"]'
        }
        query = self.query_to_format(query, 'goods')
        r = self.session.get(
            ("https://www.jky888.com/jkyun/erp/goods/getgoodsinfoandbaseunit?"
             + query),
            verify=False,
            headers=headers)
        r = r.json()
        r = pd.DataFrame(r.get('result').get('data'))
        save_date = datetime.date.today().strftime('%Y-%m-%d')
        r['DateNum'] = save_date
        r['id'] = (r['goodsId'].astype('str') + r['goodsNo'].astype('str') +
                   r['brandId'].astype('str') + r['DateNum']).apply(
                       self.hashing)
        r.to_sql("JKY_goods_list",
                 connect('bidata'),
                 index=None,
                 if_exists='append')
        return

    def get_order_details_new(self, diff_days=1, end_date_diff=1):
        end_date = (datetime.date.today() -
                    datetime.timedelta(days=end_date_diff)).strftime('%Y-%m-%d')
        start_date = (datetime.date.today() -
                      datetime.timedelta(days=diff_days)).strftime('%Y-%m-%d')
        col_map = {
            '订单标记': 'sysFlagIdArr',
            '网店订单号': 'tradeNo',
            '物流公司': 'logisticName',
            '物流单号': 'logisticNo',
            '物流上传状态': 'synStatusExplain',
            '发货截止时间': 'sndDeadLine',
            '销售渠道': 'shopName',
            '网店订单交易状态': 'tradeStatusExplain',
            '处理状态': 'curStatusExplain',
            '下单时间': 'createTime',
            '付款时间': 'payTime',
            '发货时间': 'sendTimeFromGoods',
            '客户名称': 'customerName',
            '客户账号': 'customerAccount',
            '收货人': 'receiverName',
            '手机': 'mobile',
            '收货地址': 'address',
            '客户备注': 'buyerMemo',
            '客服备注': 'sellerMemo',
            '应收邮资': 'receivedPostFee',
            '货款合计': 'totalFee',
            '应收合计': 'payment',
            'PO单号': 'subOrderNoArr',
            '平台订单类型': 'tradeTypeExplain',
            '订单确认时间': 'confirmTime',
            '电话': 'phone',
            '货品数量': 'goodsCount',
            '货品样数': 'goodsTypeCount',
            '网店优惠金额': 'favourableMoney',
            '平台优惠金额': 'platCouponFee',
            '应付佣金': 'commissionFee',
            '支付单号': 'payNo',
            '网店变更时间': 'onlineModified',
            '客户邮箱': 'customerEmail',
            '付款方式': 'chargeTypeExplain',
            '配送方式': 'sendStyleExplain',
            '平台类型': 'apiTypeExplain',
            '证件类型': 'cardTypeExplain',
            '证件名称': 'cardName',
            '证件号码': 'idCard',
            '发票类型': 'invoiceKindExplain',
            '发票抬头': 'invoiceTitle',
            '网店商品编码': 'outerId',
            '网店规格编码': 'outerSkuId',
            '交易名称': 'goodsName',
            '交易规格': 'goodsSpec',
            '数量': 'sellCount',
            '单价': 'sellPrice',
            '优惠': 'discountFee',
            '金额': 'sellTotal',
            '交易状态': 'tradeStatusExplainFromGoods',
            '退款状态': 'refundStatusExplain',
            '发货状态': 'synStatusExplainFromGoods',
            '网店子订单编号': 'subTradeNo',
            '发货物流公司': 'logisticCode',
            '发货物流单号': 'logisticPostid',
            '退款单号': 'refundNo',
            '退款时间': 'refundTime',
            '交易备注': 'goodsMemo',
            '赠品': 'isGift',
            '网店指定仓库': 'platWarehouseCode',
            '网店指定物流': 'platLogisticCode',
            '系统货品编号': 'sysGoodsNo',
            '系统品名': 'sysGoodsName',
            '系统规格': 'sysGoodsSpecName',
            '系统单位': 'sysUnit',
            '交易状态（子）': 'tradeStatusExplainFromGoods_sub',
            '退款状态（子）': 'refundStatusExplain_sub',
            '发货状态（子）': 'synStatusExplainFromGoods_sub',
            '发货时间（子）': 'sendTimeFromGoods_sub',
            '贷款合计': 'totalFee',
            'po单号': 'subOrderNoArr',
            '确认时间': 'confirmTime',
            '优惠金额': 'favourableMoney',
            '发货方式': 'sendStyleExplain',
            '网店商家编码': 'outerId',
            '网店子商家编码': 'outerSkuId'
        }
        condition = {
            'pageInfo': {
                'pageIndex': 0,
                'pageSize': 100000,
                'sortField': '',
                'sortOrder': ''
            },
            'jsonStr': {
                'gmtCreateBegin': f'{start_date} 00:00:00',
                'gmtCreateEnd': f'{end_date} 23:59:59',
            },
            'cols': [
                'tradeOnline.sysFlagIdArr', 'tradeOnline.tradeNo',
                'tradeOnline.logisticName', 'tradeOnline.logisticNo',
                'tradeOnline.synStatusExplain', 'tradeOnline.sndDeadLine',
                'tradeOnline.shopName', 'tradeOnline.tradeStatusExplain',
                'tradeOnline.curStatusExplain', 'tradeOnline.createTime',
                'tradeOnline.payTime', 'tradeOnline.sendTime',
                'tradeOnline.customerName', 'tradeOnline.customerAccount',
                'tradeOnline.receiverName', 'tradeOnline.mobile',
                'tradeOnline.address', 'tradeOnline.buyerMemo',
                'tradeOnline.sellerMemo', 'tradeOnline.receivedPostFee',
                'tradeOnline.totalFee', 'tradeOnline.payment',
                'tradeOnline.subOrderNoArr', 'tradeOnline.tradeTypeExplain',
                'tradeOnline.confirmTime', 'tradeOnline.phone',
                'tradeOnline.goodsCount', 'tradeOnline.goodsTypeCount',
                'tradeOnline.favourableMoney', 'tradeOnline.platCouponFee',
                'tradeOnline.commissionFee', 'tradeOnline.payNo',
                'tradeOnline.onlineModified', 'tradeOnline.customerEmail',
                'tradeOnline.chargeTypeExplain',
                'tradeOnline.sendStyleExplain', 'tradeOnline.apiTypeExplain',
                'tradeOnline.cardTypeExplain', 'tradeOnline.cardName',
                'tradeOnline.idCard', 'tradeOnline.invoiceKindExplain',
                'tradeOnline.invoiceTitle'
            ],
            'subCols': [
                'outerId', 'outerSkuId', 'goodsName', 'goodsSpec', 'sellCount',
                'sellPrice', 'discountFee', 'sellTotal',
                'tradeStatusExplainFromGoods', 'refundStatusExplain',
                'synStatusExplainFromGoods', 'sendTimeFromGoods', 'subTradeNo',
                'logisticCode', 'logisticPostid', 'refundNo', 'refundTime',
                'goodsMemo', 'isGift', 'platWarehouseCode', 'platLogisticCode',
                'sysGoodsNo', 'sysGoodsName', 'sysGoodsSpecName', 'sysUnit'
            ]
        }
        condition = json.dumps(condition)
        query = {
            'timestamp': f'{int(time.time() * 1000)}',
            'access_token': f'Bearer {self.session.cookies.get("token")}',
            'appkey': 'jackyun_web_browser',
            'serverName': 'omsapi-business/omsapi-business/excel',
            'excelType': '2,3',
            'sign': '',
            'headersJson':
            '[{"enName":["sysFlagIdArr","tradeNo","logisticName","logisticNo","synStatusExplain","sndDeadLine","shopName","tradeStatusExplain","curStatusExplain","createTime","payTime","sendTime","customerName","customerAccount","receiverName","mobile","address","buyerMemo","sellerMemo","receivedPostFee","totalFee","payment","subOrderNoArr","tradeTypeExplain","confirmTime","phone","goodsCount","goodsTypeCount","favourableMoney","platCouponFee","commissionFee","payNo","onlineModified","customerEmail","chargeTypeExplain","sendStyleExplain","apiTypeExplain","cardTypeExplain","cardName","idCard","invoiceKindExplain","invoiceTitle"],"showName":["订单标记","网店订单号","物流公司","物流单号","物流上传状态","发货截止时间","销售渠道","网店订单交易状态","处理状态","下单时间","付款时间","发货时间","客户名称","客户账号","收货人","手机","收货地址","客户备注","客服备注","应收邮资","货款合计","应收合计","PO单号","平台订单类型","订单确认时间","电话","货品数量","货品样数","网店优惠金额","平台优惠金额","应付佣金","支付单号","网店变更时间","客户邮箱","付款方式","配送方式","平台类型","证件类型","证件名称","证件号码","发票类型","发票抬头"]},{"enName":["outerId","outerSkuId","goodsName","goodsSpec","sellCount","sellPrice","discountFee","sellTotal","tradeStatusExplainFromGoods","refundStatusExplain","synStatusExplainFromGoods","sendTimeFromGoods","subTradeNo","logisticCode","logisticPostid","refundNo","refundTime","goodsMemo","isGift","platWarehouseCode","platLogisticCode","sysGoodsNo","sysGoodsName","sysGoodsSpecName","sysUnit"],"showName":["网店商品编码","网店规格编码","交易名称","交易规格","数量","单价","优惠","金额","交易状态","退款状态","发货状态","发货时间","网店子订单编号","发货物流公司","发货物流单号","退款单号","退款时间","交易备注","赠品","网店指定仓库","网店指定物流","系统货品编号","系统品名","系统规格","系统单位"]}]',
            'typeName': '网店订单查询',
            'multiSheet': 'true',
            'isSyn': 'true',
            'conditionJson': condition
        }
        query = self.query_to_format(query, 'orderdetails')
        details_headers = {
            'Host': 'www.jky888.com',
            'Connection': 'keep-alive',
            'Content-Length': '3967',
            'Accept': '*/*',
            'ati': f'{self.session.cookies.get("ati")}',
            'X-Requested-With': 'XMLHttpRequest',
            'Authorization': f'Bearer {self.session.cookies.get("token")}',
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.jky888.com',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer':
            'https://www.jky888.com/oms-online/shop_order_query.html?_t=873877&_winid=w3104',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }
        r = self.session.post(
            'https://www.jky888.com/jkyun/excel-service/manager/startExcelExport',
            data=query.encode().decode('latin1'),
            headers=details_headers,
            verify=False)
        r = r.json().get('result').get('data')
        r = self.session.get(r, verify=False)
        orders = pd.read_excel(io.BytesIO(r.content), sheet_name='sheet0')
        details = pd.read_excel(io.BytesIO(r.content),
                                sheet_name='sheet1',
                                dtype={"网店子订单编号": "object"})
        orders.columns = [col_map.get(i) for i in orders.columns]
        details.columns = [col_map.get(i) for i in details.columns]
        orders.drop_duplicates(inplace=True)
        details.drop_duplicates(inplace=True)
        details = details.merge(orders[['tradeNo', 'onlineModified']],
                                how='left')
        details['subTradeNo'] = details.subTradeNo.astype('str')
        details['goodsNo'] = details['outerSkuId']
        details['goodsNo'].fillna(details['outerId'], inplace=True)
        details['h_id'] = (details['tradeNo'].astype('str') +
                           details['subTradeNo'].astype('str') +
                           details['goodsNo'].astype('str') +
                           details['sellTotal'].astype('str')).apply(
                               self.hashing)
        conn = connect('bidata')
        if len(orders) > 2000:
            orders_todo = self.get_split(orders.tradeNo.to_list())
            for i in orders_todo:
                todo = "('" + "','".join(
                    orders.tradeNo.to_list()[i[0]:i[1]]) + "')"
                conn.execute(f"DELETE FROM JKY_Orders WHERE tradeNo IN {todo}")
        if len(details) > 2000:
            details_todo = self.get_split(details.subTradeNo.to_list())
            for j in details_todo:
                todo = "('" + "','".join(
                    details.h_id.to_list()[j[0]:j[1]]) + "')"
                conn.execute(
                    f"DELETE FROM JKY_Orders_Details WHERE h_id IN {todo}")
        details['is_star_goods'] = 0
        l = [['SCLMWS160323004'], ['SCLMWS181031001'],
             ['SCLMTY200403003', 'SCLMTY200403004', 'SCLMTY200403005'],
             ['SCLMWS191031001', 'SCLMWS191031002', 'SCLMWS171218003'],
             ['SCLMTM190917003', 'TU20030900002'],
             ['TU19021500001', 'TU19091100001', 'TU19021500002']]
        for i in range(len(l)):
            details['is_star_goods'][details.goodsNo.isin(l[i])] = i + 1
        details['refundStatus'] = details['refundStatusExplain'].map({
            '正常':
            1,
            '没有退款':
            1,
            '退款关闭':
            1,
            '其他':
            1,
            '卖家拒绝退款':
            1,
            '买家已经申请退款等待卖家同意':
            1,
            '卖家已经同意退款等待买家退货':
            0,
            '买家已经退货等待卖家确认收货':
            0,
            '退款成功':
            0
        })
        orders = orders[[i for i in orders.columns if i is not None]]
        orders.to_sql("JKY_Orders",
                      conn,
                      index=None,
                      if_exists='append',
                      method='multi',
                      chunksize=100)
        details.to_sql("JKY_Orders_Details",
                       conn,
                       index=None,
                       if_exists='append',
                       method='multi',
                       chunksize=100)
        return 'Success!'

    def query_to_format(self, query, mode):
        sign = self.get_sign(query)
        query.update({"sign": sign, "_": str(int(time.time() * 1000))})
        query = '&'.join([
            '='.join(i if i[0] not in (
                ['cols'] if mode == 'order' else ['access_token', 'cols']
            ) else [i[0], parse.quote(i[1])]) for i in query.items()
        ])
        return query

    def refresh_token(self):
        r = self.session.post(
            "https://www.jky888.com/auth/refresh",
            data=f"refreshToken={self.session.cookies.get('refresh_token')}",
            verify=False,
            headers={
                'authority': 'www.jky888.com',
                'content-length': '1133',
                'authorization': f'Bearer {self.session.cookies.get("token")}',
                'accept': '*/*',
                'ati': '3012930807395',
                'x-requested-with': 'XMLHttpRequest',
                'clientid': 'jackyun_web_browser',
                'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
                'content-type':
                'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://www.jky888.com',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer': 'https://www.jky888.com/',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9'
            })
        r = r.json()
        self.session.cookies.update({
            "token": r.get('access_token'),
            "refresh_token": r.get('refresh_token')
        })
        return

    def get_shipment_data(self, diff_days=1):
        dts, dte = (datetime.date.today() -
                    datetime.timedelta(days=diff_days)).strftime("%Y-%m-%d"), (
                        datetime.date.today() -
                        datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        def get_query(pageSize=1000, pageNo=0):
            query = {
                'timestamp':
                f'{int(time.time()*1000)}',
                'access_token':
                f'Bearer {self.session.cookies.get("token")}',
                'appkey':
                'jackyun_web_browser',
                'sign':
                '',
                'orderArchive':
                '0',
                'cfgArchiveBeginTime':
                f'{dts} 00:00:00',
                'cfgArchiveEndTime':
                f'{dte} 23:59:59',
                'pageIndex':
                f'{pageNo}',
                'pageSize':
                f'{pageSize}',
                'sortField':
                '',
                'sortOrder':
                '',
                'cols':
                '["orderNo","flagIds","orderStatusName","erporderNo","sendTime","operationFinishTime","orderTypeName","warehouseName","ownerName","logisticName","logisticNo","goodsTotal","goodsKinds","goodsList","buyerMemo","sellerMemo","deliverGoodsPrintStatus","waybillPrintStatus","summaryPrintStatus","waveNo","province","city","district","shopName","platName","tradeTypeMsg","country","street","orderTime","payTime","deadSendTime","buyerName","buyerNick","platOrderNo","phoneAndMobile","address","platformCode"]'
            }
            query = self.query_to_format(query, 'shipment')
            return query

        headers = {
            'authority': 'www.jky888.com',
            'content-length': '1021',
            'accept': 'text/plain, */*; q=0.01',
            'ati': '3012930807395',
            'x-requested-with': 'XMLHttpRequest',
            'authorization': f'Bearer {self.session.cookies.get("token")}',
            'user-agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://www.jky888.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer':
            'https://www.jky888.com/wms/outbound_operation/invoice_search_list.html?_t=925941&_winid=w4194',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9'
        }
        first_query = get_query()
        total = self.session.post(
            "https://www.jky888.com/jkyun/wms/orderlist/count",
            data=first_query,
            headers=headers).json().get('result').get('data')
        r = self.session.post(
            "https://www.jky888.com/jkyun/wms/orderlist/list",
            data=first_query,
            headers=headers)
        r = r.json().get('result').get('data')
        total -= len(r)
        page = 1
        while total > 0:
            temp = self.session.post(
                "https://www.jky888.com/jkyun/wms/orderlist/list",
                data=get_query(pageNo=page),
                headers=headers).json().get('result').get('data')
            total -= len(temp)
            r += temp
            page += 1
        r = pd.DataFrame(r)
        return r

    def get_shipment_details(self, orderNo):
        query = {
            'timestamp': f'{int(time.time()*1000)}',
            'access_token': f'Bearer {self.session.cookies.get("token")}',
            'appkey': 'jackyun_web_browser',
            'sign': '',
            'orderId': f'{orderNo}',
            'orderArchive': '0',
            'pageIndex': '0',
            'pageSize': '50',
            'sortField': '',
            'sortOrder': '',
            'cols':
            '["goodsNo","goodsName","skuName","sellCount","unit","skuBarcode","brandName","positionsName","remark","batchNo","snList"]',
            '_': f'{int(time.time()*1000)}'
        }
        query = self.query_to_format(query, 'shipment')
        url = f'https://www.jky888.com/jkyun/wms/orderlist/detaillist?{query}'
        headers = {
            'authority': 'www.jky888.com',
            'accept': 'text/plain, */*; q=0.01',
            'ati': '3012930807395',
            'x-requested-with': 'XMLHttpRequest',
            'authorization': f'Bearer {self.session.cookies.get("token")}',
            'user-agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer':
            'https://www.jky888.com/wms/outbound_operation/invoice_search_list.html?_t=925941&_winid=w4194',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9'
        }
        r = self.session.get(url, headers=headers)
        r = r.json().get('result').get('data')
        return r

    def get_shipments(self, diff_days=1):
        parent = self.get_shipment_data(diff_days)
        details = []
        for i in parent.orderId:
            details += self.get_shipment_details(i)
        details = pd.DataFrame(details)
        return parent, details

    def get_zh_goods(self):
        query = {
            'timestamp': f'{int(time.time()*1000)}',
            'access_token': f'Bearer {self.session.cookies.get("token")}',
            'appkey': 'jackyun_web_browser',
            'sign': '88B91756CB5F2E0B76ED2D2F369B0AB4',
            'blockUp': '0',
            'packageGood': '1',
            'pageIndex': '0',
            'pageSize': '200',
            'cols':
            '["goodsNo","goodsName","skuProperitesName","cateName","brandName","unitName","skuBarcode","flagData","skuWeight","skuLength","skuWidth","skuHeight","volume","retailPrice","creator","modifiedBy","auditor"]',
            '_': f'{int(time.time()*1000)}'
        }
        sign = self.get_sign(query)
        query['sign'] = sign
        query = self.query_to_format(query, 'other')
        r = self.session.get(
            f'https://www.jky888.com/jkyun/erp/goods/getskulistyycondition?{query.encode().decode("latin1")}',
            headers={
                'authority': 'www.jky888.com',
                'accept': 'text/plain, */*; q=0.01',
                'ati': '1269360998052',
                'x-requested-with': 'XMLHttpRequest',
                'authorization': f'Bearer {self.session.cookies.get("token")}',
                'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer':
                'https://www.jky888.com/erp/goods_file_erp/goods_managet_combination.html?_t=896130&_winid=w4495',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8'
            },
            verify=False)
        r = r.json()
        r = r.get('result').get('data')
        r = pd.DataFrame(r)
        return r

    def get_zh_goods_details(self, goodsId, sku):
        query = {
            'timestamp': f'{int(time.time()*1000)}',
            'access_token': f'Bearer {self.session.cookies.get("token")}',
            'appkey': 'jackyun_web_browser',
            'sign': '2813E0F4427A91D100697A6AB5E4973A',
            'goodsId': f'{goodsId}',
            'pageIndex': '0',
            'pageSize': '50',
            'cols':
            '["goodsNo","goodsName","skuProperitesName","skuBarcode","imgUrl","skuLength","skuWidth","skuHeight","skuWeight","unitName","goodsAmount","sharePrice","shareAmount","shareRatio","retailPrice"]',
            '_': f'{int(time.time()*1000)}',
        }
        sign = self.get_sign(query)
        query['sign'] = sign
        query = self.query_to_format(query, 'other')
        r = self.session.get(
            f'https://www.jky888.com/jkyun/erp/package/getPackageId?{query.encode().decode("latin1")}',
            headers={
                'authority': 'www.jky888.com',
                'accept': 'text/plain, */*; q=0.01',
                'ati': '1269360998052',
                'x-requested-with': 'XMLHttpRequest',
                'authorization': f'Bearer {self.session.cookies.get("token")}',
                'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer':
                'https://www.jky888.com/erp/goods_file_erp/goods_managet_combination.html?_t=896130&_winid=w4495',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8'
            },
            verify=False)
        r = r.json()
        r = pd.DataFrame(r.get('result').get('data'))
        r['sku'] = sku
        return r

    def get_zh(self):
        zh_list = self.get_zh_goods()
        details = pd.DataFrame()
        for i in zh_list[['goodsId', 'goodsNo']].values:
            temp = self.get_zh_goods_details(i[0], i[1])
            details = pd.concat([details, temp])
        zh_list['updateTime'] = datetime.datetime.today().strftime(
            '%Y-%m-%d %H:%M:%S')
        details['updateTime'] = datetime.datetime.today().strftime(
            '%Y-%m-%d %H:%M:%S')
        con = connect('bidata')
        zh_list.to_sql("JKY_ZH_list", con, index=None, if_exists='append')
        details.to_sql("JKY_ZH_Details", con, index=None, if_exists='append')
        return
    
    def stock_in_run(self, diff_days=1):
        parent = self.get_stock_in_data(diff_days=diff_days)
        if len(parent)>0:
            con = connect('bidata')
            children = pd.DataFrame()
            for i in parent.docId:
                children = pd.concat([children, self.get_stock_in_details(i)])
            parent = self.format_stock_in(parent)
            parent.to_sql("JKY_Stock_In", con, index=None, if_exists='append')
            children.to_sql("JKY_Stock_In_Details", con, index=None, if_exists='append')
        return

    def get_stock_in_data(self, diff_days=1):
        query = {
            'timestamp': f'{int(time.time()*1000)}',
            'access_token': f'Bearer {self.session.cookies.get("token")}',
            'appkey': 'jackyun_web_browser',
            'sign': '104410AD7F1FE13B17434D2EE1762AD8',
            'ownerId': '960025061393039872',
            'ownerName': '647102',
            'serviceType': 'goodsdoc.search',
            'inOrOut': '1',
            'archived': '0',
            'inOutDateStart': f'{(datetime.datetime.today()-datetime.timedelta(days=diff_days)).strftime("%Y-%m-%d")} 00:00:00',
            'inOutDateEnd': f'{(datetime.datetime.today()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")} 23:59:59',
            'inouttypeHeader': '$in$101',
            'pageIndex': '0',
            'pageSize': '200',
            'sortField': '',
            'sortOrder': '',
            'cols':
            '["goodsdocNo","deliveryNo","inOutDate","inouttypeName","warehouseName","billNo","financeBillStatus","companyName","createUserName","vendCustomerName","inOutReason","flagData","callbackStatusName","receiveGoodsRemark","goodsdocRemark","send","sendTel","sendPhone","sendEmail","sendAddress","receive","receiveTel","receivePhone","receiveEmail","receiveAddress"]',
            'fieldNames': 'goodsdoc_no,warehouse_name,bill_no',
            'value': ''
        }
        sign = self.get_sign(query)
        query['sign'] = sign
        query = self.query_to_format(query, 'other')
        r = self.session.post(
            f"https://www.jky888.com/jkyun/erp-stock/search/pagelist1",
            data=query,
            headers={
                'authority': 'www.jky888.com',
                'content-length': '962',
                'accept': 'text/plain, */*; q=0.01',
                'ati': '3012930807395',
                'x-requested-with': 'XMLHttpRequest',
                'authorization': f'Bearer {self.session.cookies.get("token")}',
                'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
                'content-type':
                'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://www.jky888.com',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer':
                'https://www.jky888.com/erp/stock/stockin_list_mode.html?mode=order&_t=584328&_winid=w2314',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9'
            })
        r = r.json()
        r = r.get('result').get('data')
        r = pd.DataFrame(r)
        return r

    def get_stock_in_details(self, doc_id):
        query = {
            'timestamp':
            f'{int(time.time()*1000)}',
            'access_token':
            f'Bearer {self.session.cookies.get("token")}',
            'appkey':
            'jackyun_web_browser',
            'sign':
            '337034644E3A112E9D12D6C8195966E6',
            'archived':
            '0',
            'docId':
            doc_id,
            'ownerId':
            '960025061393039872',
            'ownerName':
            '647102',
            'serviceType':
            'goodsdoc.detail.search',
            'pageIndex':
            '0',
            'pageSize':
            '50',
            'sortField':
            '',
            'sortOrder':
            '',
            'cols':
            '["goodsNo","goodsName","skuName","skuBarcode","unitName","quantity","baceCurrencyCostPrice","baceCurrencyCostAmount","isCertified","batchNo","serialNo","batchNumber","manufacturer","approvalNumber","productionDate","expirationDate","shelfLife","shelfLiftUnit","goodsDetailRemark","goodsField1","goodsField2"]'
        }
        sign = self.get_sign(query)
        query['sign'] = sign
        query = self.query_to_format(query, 'other')
        r = self.session.post(
            f"https://www.jky888.com/jkyun/erp-stock/search/pagelist1",
            data=query,
            headers={
                'authority': 'www.jky888.com',
                'content-length': '962',
                'accept': 'text/plain, */*; q=0.01',
                'ati': '3012930807395',
                'x-requested-with': 'XMLHttpRequest',
                'authorization': f'Bearer {self.session.cookies.get("token")}',
                'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
                'content-type':
                'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://www.jky888.com',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer':
                'https://www.jky888.com/erp/stock/stockin_list_mode.html?mode=order&_t=584328&_winid=w2314',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9'
            })
        r = r.json()
        r = pd.DataFrame(r.get('result').get('data'))
        r['docId'] = doc_id
        return r
    
    # 1.按月获取发货销售订单明细数据
    def get_monthly_order_shipment_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'jsonStr': '{"selectTimeStr":"tradeOrder.consign_time","timeBegin":"%s","timeEnd":"%s","customerName":"","customerType":[],"companyId":[],"shopId":[],"tradeType":[],"tradeFrom":[],"includeFlagId":[],"tradeStatus":[],"skuId":"","cateId":[],"warehouseId":[],"logisticType":"","logisticId":[],"sellerId":"","unitPriceStart":"","unitPriceEnd":"","isCancel":"0","isFit":"","assemblyDimension":0,"goodsMemoFlag":"-1","goodsMemo":null,"chargeAccounts":[],"chargeTypes":[],"tradeNo":[],"customerAccount":"","isSafeMode":true}'%(sdt, edt),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols': '["sourceTradeNo","tradeFrom","chargeCurrency","shopId","unit","customerAccount","tradeNo","shopName","brandName","goodsNo","goodsName","specName","sellCount","sellPrice","customerName","logisticName","mainPostid","warehouseName","receiverName","address","barcode","discountFee","sellTotal","cost","afterShareUnitFee","shareFavourableFee","afterShareFee","otherShareFavourableFee","grossProfit","grossProfitRate","unGrossProfit","unGrossProfitRate","tradeTypeExplain","tradeFromExplain","tradeStatusExplain","sysFlagIds","cateName","untaxedUnitFee","untaxedFee","localAfterShareFee","packageWeight","customerCode","taxFee","logisticTypeExplain","seller","mobile","phone","tradeTime","gmtCreate","payTime","auditTime","consignTime","customerPrice","customerTotal","goodsMemo","ownerName","billCheckTypeExplain","estimateWeight","postFee","goodsReceivablePostage","sellerMemo","buyerMemo","companyName","departName","customerGradeName","chargeTypeExplain","accountName","payNo","customerTypeName","endCustomerAccount","terminalSalesNo","agentTradeNo","country","state","city","district","platGoodsId","tradeGoodsNo","tradeGoodsName","tradeGoodsSpec","tradeGoodsUnit","appraisePrice","appraiseCost","appraiseGrossProfit","customerTradeNo","customerSubtradeNo","sourceSubtradeNo","baseGoods.colorName","baseGoods.sizeName","baseGoods.materialName","goodsAlias","goodsEnName","supplierItemNo","serialNo","settleStatusExplain","batchNo","baseGoods.skuNo","baseGoods.goodsField1","baseGoods.goodsField2","minPrice","price3","price2","price1"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/oms/trade/detailCount'
        detail_url = 'https://www.jky888.com/jkyun/oms/trade/tradeOrderDetialList'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '3245',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/oms/order/order_detail_list.html?_t=456939&_winid=w2331',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
        
    
    # 2.按月获取售后发货明细数据
    def get_monthly_aftersale_shipment_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'method':'differ.ass.exchange.getexchangedetailreportpaging',
                        'bizContent':'{"queryNoType":"1","queryNoList":"","timeType":"2","dateBegin":"%s","dateEnd":"%s","reasonDesc":[],"customerId":"","customerName":"","shopIds":[],"sources":[],"flagIds":[],"companyIds":[],"exchangeStatus":[],"goodsNo":"","barcode":"","skuIds":[],"cateIds":[],"brandIds":[],"warehouseIds":[],"responsiblePersonCodes":[],"sourcePayTimeBegin":"","sourcePayTimeEnd":"","consignTimeBegin":"","consignTimeEnd":"","isCancel":"0","problemDesc":"","customerRemark":"","serviceRemark":"","queryType":2,"pageIndex":%d,"pageSize":1000}'%(sdt, edt,pg_idx),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["customerAccount","exchangeNo","shopName","reasonDesc","goodsNo","goodsName","specName","unit","price","sendCount","sendDiscounts","sendFee","shareFavourableFee","shareFavourableTotalFee","cost","grossProfit","grossProfitRate","postFee","responsiblePersonDesc","exchangeStatus","barcode","flagIds","sourceExplain","gmtCreate","auditTime","settlementDate","deliveryPerson","registrant","auditor","sourceTradeNo","targetNo","sourceExchangeNo","tradeNo","brandName","cateName","remark","customerName","mobile","applyTime","sourcePayTime","consignTime","completeTime","sourceWarehouseName","appendMemo","platGoodsId","problemDesc","customerRemark","serviceRemark","goodsField1","goodsField2"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/ass-business/open/front/total'
        detail_url = 'https://www.jky888.com/jkyun/ass-business/open/front/call'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '2185',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/ass/afterSaleReport/report_detail_query.html?queryType=exchange_send_report_detail_query&_t=19661&_winid=w4291',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
       
        result = [json.loads(i.json().get('result').get('data')).get('items') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
    # 3.按月获取售后退货明细数据
    def get_monthly_aftersale_return_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'method':'differ.ass.exchange.getexchangedetailreportpaging',
                        'bizContent':'{"queryNoType":"1","queryNoList":"","timeType":"2","dateBegin":"%s","dateEnd":"%s","reasonDesc":[],"customerId":"","customerName":"","shopIds":[],"logisticIds":[],"sources":[],"flagIds":[],"companyIds":[],"exchangeStatus":[],"goodsNo":"","barcode":"","skuIds":[],"cateIds":[],"brandIds":[],"warehouseIds":[],"responsiblePersonCodes":[],"sourcePayTimeBegin":"","sourcePayTimeEnd":"","consignTimeBegin":"","consignTimeEnd":"","isCancel":"0","problemDesc":"","customerRemark":"","serviceRemark":"","queryType":1,"pageIndex":%d,"pageSize":1000}'%(sdt, edt,pg_idx),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["customerAccount","exchangeNo","shopName","reasonDesc","goodsNo","goodsName","specName","unit","price","returnCount","deliveryCount","returnDiscounts","returnFee","cost","shareFavourableFee","shareFavourableTotalFee","grossProfit","grossProfitRate","responsiblePersonDesc","exchangeStatus","barcode","deliveryPerson","registrant","auditor","mobile","sourceTradeNo","targetNo","tradeNo","sourceExchangeNo","brandName","cateName","remark","customerName","sourceExplain","flagIds","logisticName","mainPostid","sourceMainPostid","gmtCreate","applyTime","auditTime","settlementDate","deliveryTime","sourcePayTime","consignTime","completeTime","warehouseName","sourceWarehouseName","sourceLogisticName","picker","packer","boxuper","allocator","appendMemo","platGoodsId","deliveryCompleteRemark","problemDesc","customerRemark","serviceRemark","goodsField1","goodsField2"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/ass-business/open/front/total'
        detail_url = 'https://www.jky888.com/jkyun/ass-business/open/front/call'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '2185',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/ass/afterSaleReport/report_detail_query.html?queryType=exchange_send_report_detail_query&_t=19661&_winid=w4291',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        
        result = [json.loads(i.json().get('result').get('data')).get('items') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
    
    # 4.按月获取补偿退款明细数据
    def get_monthly_compensate_refund_return_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'method':'differ.ass.refund.getrefunddetailreport',
                        'bizContent':'{"queryType":"1","queryList":"","reportTimeType":"2","reportTimeBegin":"%s","reportTimeEnd":"%s","companyIds":[],"reasonDescs":[],"customerId":"","customerName":"","shopIds":[],"orderSourceList":[],"flagIds":[],"refundStatusList":[],"goodsNo":"","barcode":"","skuIdList":"","cateIdList":[],"brandIdList":[],"responsiblePersonCodes":[],"supplierIds":[],"sourcePayTimeBegin":"","sourcePayTimeEnd":"","sourceDeliverTimeBegin":"","sourceDeliverTimeEnd":"","bCancel":"0","problemDesc":"","customerRemark":"","serviceRemark":"","pageIndex":%d,"pageSize":1000}'%(sdt, edt,pg_idx),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["customerAccount","refundNo","shopName","reasonDesc","goodsNo","goodsName","specName","unit","price","refundFee","shareFee","shareReturnTotal","grossProfit","grossProfitRate","responsiblePersonDesc","refundStatusExplain","barcode"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/ass-business/open/front/total'
        detail_url = 'https://www.jky888.com/jkyun/ass-business/open/front/call'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '2185',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/ass/afterSaleReport/report_detail_query.html?queryType=refund_report_detail_query&_t=219054&_winid=w4291',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        
        result = [json.loads(i.json().get('result').get('data')).get('items') for i in result]
        try:
            result = reduce(lambda x,y: x+y, result)
        except TypeError:
            result = []
        result = pd.DataFrame(result)
        return result
    
    # 5.按月获取错漏补发明细数据
    def get_monthly_error_send_return_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'method':'differ.ass.disorder.getdisordersenddetailreportpaging',
                        'bizContent':'{"queryType":"1","queryList":"","timeType":"2","dateBegin":"%s","dateEnd":"%s","reasonDesc":[],"customerId":"","customerName":"","shopIds":[],"flagIds":[],"disorderStatus":[],"goodsNo":"","barcode":"","skuIds":[],"cateIds":[],"brandIds":[],"dutyPersonIds":[],"supplierIds":[],"isCancel":"0","problemDesc":"","customerRemark":"","serviceRemark":"","pageIndex":%d,"pageSize":1000}'%(sdt, edt,pg_idx),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["customerAccount","reasonDesc","disorderNo","shopName","goodsNo","goodsName","specName","unit","price","count","discountFee","sellTotal","dutyPersonName","brandName"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/ass-business/open/front/total'
        detail_url = 'https://www.jky888.com/jkyun/ass-business/open/front/call'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '2185',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/ass/afterSaleReport/report_detail_query.html?queryType=error_send_report_detail_query&_t=511658&_winid=w4291',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        
        result = [json.loads(i.json().get('result').get('data')).get('items') for i in result]
        try:
            result = reduce(lambda x,y: x+y, result)
            
        except TypeError:
            result = []
    
        result = pd.DataFrame(result)
        return result
    
    # 6.按月获期初库存明细数据
    def get_monthly_history_warehouse_sdt_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'goodsCateIds':'',
                        'skuId':'',
                        'brandId':'',
                        'warehouseIds':'968606409259025152,965712961844183936,1150761981069525632,1025243866625639296,1017292810616734592,978065461043069312,970814096235365888,967961503880184448,960824757136982528',
                        'createEnd':'%s'%(sdt),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["warehouseName","goodsNo","goodsName","skuName","unitName","skuBarcode","brandName","cateName","isCertified","quantity"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/erp/goodsdoc/historystocktotal'
        detail_url = 'https://www.jky888.com/jkyun/erp/goodsdoc/historystock'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '2185',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/erp/stock/history_warehouse_erp.html?_winid=w9070&_t=291886',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)

        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
     # 7.按月获期末库存明细数据
    def get_monthly_history_warehouse_edt_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'goodsCateIds':'',
                        'skuId':'',
                        'brandId':'',
                        'warehouseIds':'968606409259025152,965712961844183936,1150761981069525632,1025243866625639296,1017292810616734592,978065461043069312,970814096235365888,967961503880184448,960824757136982528',
                        'createEnd':'%s'%(edt),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["warehouseName","goodsNo","goodsName","skuName","unitName","skuBarcode","brandName","cateName","isCertified","quantity"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/erp/goodsdoc/historystocktotal'
        detail_url = 'https://www.jky888.com/jkyun/erp/goodsdoc/historystock'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '2185',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/erp/stock/history_warehouse_erp.html?_winid=w9070&_t=291886',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)

        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
     # 8.按月获出入库明细账明细数据
    def get_monthly_in_out_warehouse_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                    'access_token': f'Bearer {self.session.cookies.get("token")}',
                    'appkey': 'jackyun_web_browser',
                    'sign': '',
                    'inOutDateType':'1',
                    'timeType':'3',
                    'trDateStart':'%s'%(sdt),
                    'trDateEnd':'%s'%(edt),
                    'getType':'1',
                    'billNos':'',
                    'warehouseId':'',
                    'channelIds':'',
                    'skuId':'',
                    'goodsNo':'',
                    'goodsName':'',
                    'skuName':'',
                    'skuBarcode':'',
                    'brandId':'',
                    'cateId':'',
                    'inOutType':'',
                    'detailStatus':'',
                    'userName':'',
                    'vendCustomerName':'',
                    'applyCompanyId':'',
                    'applyDepartId':'',
                    'applyUserId':'',
                    'inOutReason':'',
                    'isHideWriteOffBill':'0',
                    'ownerId':'960025061393039872',
                    'ownerName':'647102',
                    'pageIndex': str(pg_idx),
                    'pageSize': '1000',
                    'sortField':'',
                    'sortOrder':'',
                    'cols':'["vendCustomerName","billNo","sourceBillNo","deliveryNo","warehouseName","trDate","systemTrDate","inouttypeName","goodsdocNo","goodsNo","goodsName","skuName","skuBarcode","channelName","retailPrice","retailAmtIn","retailAmtOut","unitName","isCertified","inCuPrice","inQuantity","inCuValue","outCuPrice","outQuantity","outCuValue","companyName","userName","memo","applyMemo","rowRemark","comment","inOutReason","settFeeAmount","estCost","estTax","applyCompanyName","applyDepartName","applyUserName","cateName","brandName","goodsAlias","goodsNameEn","outBillNo","batchNo","serialNo","goodsField1","goodsField2"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/erp/goodsdoc/getcuvalue'
        detail_url = ' https://www.jky888.com/jkyun/erp/goodsdoc/getgoodsdoc'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '1473',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/erp/stock/in_out_warehouse_detail.html?_t=801987&_winid=w4695',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data').get('total'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        # result = [json.loads(i.json().get('result').get('data')).get('items') for i in result]
        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        
        return result
        
    
    
    # 9.按月获取之月份已发货未完成上月完成订单明细数据
    def get_monthly_order_shipment_lastmonth_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'jsonStr':'{"selectTimeStr":"tradeOrder.consign_time","timeBegin":"%s","timeEnd":"%s","customerName":"","customerType":[],"companyId":[],"shopId":[],"tradeType":[],"tradeFrom":[],"includeFlagId":[],"tradeStatus":["9090"],"skuId":"","cateId":[],"warehouseId":[],"logisticType":"","logisticId":[],"sellerId":"","unitPriceStart":"","unitPriceEnd":"","isCancel":"0","isFit":"","assemblyDimension":0,"goodsMemoFlag":"-1","goodsMemo":null,"chargeAccounts":[],"chargeTypes":[],"tradeNo":[],"customerAccount":"","isSafeMode":true}'%(sdt, edt),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["sourceTradeNo","tradeFrom","chargeCurrency","shopId","unit","customerAccount","customerName","tradeNo","shopName","logisticName","mainPostid","extraLogisticNo","warehouseName","receiverName","address","goodsNo","goodsName","specName","barcode","sellCount","sellPrice","discountFee","sellTotal","cost","afterShareUnitFee","shareFavourableFee","afterShareFee","otherShareFavourableFee","grossProfit","grossProfitRate","unGrossProfit","unGrossProfitRate","tradeTypeExplain","tradeFromExplain","tradeStatusExplain","sysFlagIds","brandName","cateName","untaxedUnitFee","untaxedFee","localAfterShareFee","packageWeight","customerCode","taxFee","logisticTypeExplain","seller","merchandiser","mobile","phone","tradeTime","gmtCreate","payTime","auditTime","consignTime","customerPrice","customerTotal","goodsMemo","ownerName","billCheckTypeExplain","estimateWeight","postFee","goodsReceivablePostage","sellerMemo","buyerMemo","companyName","departName","customerGradeName","chargeTypeExplain","accountName","payNo","customerTypeName","endCustomerAccount","terminalSalesNo","agentTradeNo","country","state","city","district","platGoodsId","tradeGoodsNo","tradeGoodsName","tradeGoodsSpec","tradeGoodsUnit","appraisePrice","appraiseCost","appraiseGrossProfit","customerTradeNo","customerSubtradeNo","sourceSubtradeNo","baseGoods.colorName","baseGoods.sizeName","baseGoods.materialName","goodsAlias","goodsEnName","supplierItemNo","serialNo","settleStatusExplain","batchNo","baseGoods.skuNo","finReceiptTime","finReceiptStatusExplain","baseGoods.goodsField1","baseGoods.goodsField2","minPrice","price3","price2","price1"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/oms/trade/detailCount'
        detail_url = 'https://www.jky888.com/jkyun/oms/trade/tradeOrderDetialList'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '3245',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/oms/order/order_detail_list.html?_t=261024&_winid=w5725',

                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
    # 10.按月获取发货销售订单明细数据
    def get_monthly_order_shipment_way_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'jsonStr':'{"selectTimeStr":"tradeOrder.consign_time","timeBegin":"2020-08-01 00:00:00","timeEnd":"%s","customerName":"","customerType":[],"companyId":[],"shopId":[],"tradeType":[],"tradeFrom":[],"includeFlagId":[],"tradeStatus":["6000"],"skuId":"","cateId":[],"warehouseId":[],"logisticType":"","logisticId":[],"sellerId":"","unitPriceStart":"","unitPriceEnd":"","isCancel":"0","isFit":"","assemblyDimension":0,"goodsMemoFlag":"-1","goodsMemo":null,"chargeAccounts":[],"chargeTypes":[],"tradeNo":[],"customerAccount":"","isSafeMode":true}'%(edt),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols': '["sourceTradeNo","tradeFrom","chargeCurrency","shopId","unit","customerAccount","tradeNo","shopName","brandName","goodsNo","goodsName","specName","sellCount","sellPrice","customerName","logisticName","mainPostid","warehouseName","receiverName","address","barcode","discountFee","sellTotal","cost","afterShareUnitFee","shareFavourableFee","afterShareFee","otherShareFavourableFee","grossProfit","grossProfitRate","unGrossProfit","unGrossProfitRate","tradeTypeExplain","tradeFromExplain","tradeStatusExplain","sysFlagIds","cateName","untaxedUnitFee","untaxedFee","localAfterShareFee","packageWeight","customerCode","taxFee","logisticTypeExplain","seller","mobile","phone","tradeTime","gmtCreate","payTime","auditTime","consignTime","customerPrice","customerTotal","goodsMemo","ownerName","billCheckTypeExplain","estimateWeight","postFee","goodsReceivablePostage","sellerMemo","buyerMemo","companyName","departName","customerGradeName","chargeTypeExplain","accountName","payNo","customerTypeName","endCustomerAccount","terminalSalesNo","agentTradeNo","country","state","city","district","platGoodsId","tradeGoodsNo","tradeGoodsName","tradeGoodsSpec","tradeGoodsUnit","appraisePrice","appraiseCost","appraiseGrossProfit","customerTradeNo","customerSubtradeNo","sourceSubtradeNo","baseGoods.colorName","baseGoods.sizeName","baseGoods.materialName","goodsAlias","goodsEnName","supplierItemNo","serialNo","settleStatusExplain","batchNo","baseGoods.skuNo","baseGoods.goodsField1","baseGoods.goodsField2","minPrice","price3","price2","price1"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/oms/trade/detailCount'
        detail_url = 'https://www.jky888.com/jkyun/oms/trade/tradeOrderDetialList'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '3245',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'https://www.jky888.com/oms/order/order_detail_list.html?_t=261024&_winid=w5725',

                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
    
    # 11.按月获取菜鸟销售单明细数据
    def get_monthly_cainiao_salesorder_data(self, year, month):
        sdt, edt = self.getFirstAndLastDay(year, month)
        def genarate_query(self, pg_idx, sdt, edt):
            data = {'timestamp': str(int(time.time()*1000)),
                        'access_token': f'Bearer {self.session.cookies.get("token")}',
                        'appkey': 'jackyun_web_browser',
                        'sign': '',
                        'jsonStr':'{"isSafeMode":true,"queryPageFrom":0,"hasQueryHistory":0,"gmtCreatedBegin":"2020-09-01 00:00:00","gmtCreatedEnd":"%s","timeType":"4","timeBegin":"%s","timeEnd":"%s","settleStatus":[],"shopId":[],"warehouseId":[],"lastShipTimeBegin":"","lastShipTimeEnd":"","includeFlagId":[],"customerName":"","tradeType":[],"logisticStatus":[],"hasGift":"-1","tradeNo":[]}'%(edt,sdt, edt),
                        'pageIndex': str(pg_idx),
                        'pageSize': '1000',
                        'sortField': '',
                        'sortOrder': '',
                        'cols':'["tradeOrder.orderNo","tradeOrder.tradeStatus","tradeOrder.payment","tradeOrderInvoice.invoiceStatus","tradeOrder.warehouseId","tradeOrder.flagIds","tradeOrder.tradeCount","tradeOrder.consignTime","tradeOrder.tradeTime","tradeOrder.confirmTime","tradeOrder.tradeType","tradeOrder.settleStatus","tradeOrderCustomer.customerName","tradeOrderCustomer.email","tradeOrder.lastShipTime","tradeOrderCustomer.customerId","sourceTradeNo","tradeOrder.tradeFrom","tradeOrder.shopId","tradeOrder.tradeNo","tradeOrder.tradeStatusExplain","tradeOrder.shopName","tradeOrder.tradeFromExplain","tradeOrder.handleTime","tradeOrder.payTime","tradeOrder.warehouseName","tradeOrder.logisticName","tradeOrder.mainPostid","tradeOrder.tradeTypeExplain","tradeOrder.totalFee","tradeOrder.seller","tradeOrder.goodslist","tradeOrderCustomer.customerAccount","tradeOrderReceiver.receiverName","tradeOrderReceiver.mobile","tradeOrderReceiver.phone","tradeOrderReceiver.address","mergeRemarks","tradeOrder.logisticStatusExplain","tradeOrder.settleStatusExplain","tradeOrder.distributionStation","tradeOrder.gmtCreate","tradeOrder.arriveTime","tradeOrder.payStatusExplain","tradeOrder.payDueDate","extraLogisticNo","platLogisticName","platLogisticCode","platWarehouseCode","tradeOrderAgentExtra.agentTradeNo","tradeOrderAgentExtra.terminalSalesNo","targetTradeAfterNo","sourceTradeAfterNo","tradeOrder.auditTime","tradeOrder.reviewTime","tradeOrder.notifyPickTime","tradeOrder.logisticTypeExplain","tradeOrder.taxFee","tradeOrder.discountFee","localPayment","tradeOrder.receivedPostFee","tradeOrder.postFee","tradeOrder.warehouseFee","tradeOrder.otherFee","tradeOrder.couponFee","tradeOrder.grossProfit","tradeOrderFin.billDate","tradeOrderFin.billCheckTypeExplain","tradeOrderFin.checkTotal","tradeOrder.accountName","tradeOrder.chargeTypeExplain","tradeOrder.chargeCurrency","tradeOrder.payNo","tradeOrder.buyerMemo","tradeOrder.sellerMemo","tradeOrder.merchandiser","tradeOrder.register","tradeOrder.auditor","tradeOrder.reviewer","tradeOrder.estimateWeight","tradeOrder.estimateVolume","tradeOrder.packageWeight","tradeOrder.goodsTypeCount","tradeOrder.stockoutNo","tradeOrderAgentExtra.supplierName","tradeOrderInvoice.invoiceTypeExplain","tradeOrderInvoice.payerName","tradeOrderInvoice.payerRegno","tradeOrderInvoice.invoiceStatusExplain","tradeOrderInvoice.invoiceAmount","tradeOrderInvoice.invoicedAmount","tradeOrderInvoice.invoiceNo","tradeOrderPre.preTypedetail","tradeOrderPre.firstPaytime","tradeOrderPre.finalPaytime","tradeOrderPre.activationTime","tradeOrderReceiver.identityCardTypeExplain","tradeOrderReceiver.identityCardNo","tradeOrderReceiver.identityCardName","tradeOrderReceiver.country","tradeOrderReceiver.state","tradeOrderReceiver.city","tradeOrderReceiver.district","tradeOrderReceiver.town","tradeOrderCustomer.endCustomerAccount","goodsImgInfo","onlineImgInfo","tradeOrder.appendMemo","tradeCost","grossProfitRate","sourceSubtradeNo","tradeOrder.customerPayment","tradeOrder.customerDiscountFee","tradeOrder.customerPostFee","tradeOrder.customerTotalFee","tradeOrder.uploadMarketExplain","tradeOrder.expectReachTime","tradeOrder.distributeLimitationExplain","tradeOrder.customerPayNo","tradeOrder.customerTaxFee","tradeOrder.cancelReason","tradeOrder.insuredValue","tradeOrderAgentExtra.agentShopName","tradeOrder.pickupTime","platName","realFee","tradeOrder.sourceTradeType","tradeOrder.salesName","tradeOrder.completeTime"]'}
            data['sign'] = self.get_sign(data)
            data = '&'.join(["=".join(i) for i in data.items()])
            return data
        page_info_url = 'https://www.jky888.com/jkyun/oms/trade/count'
        detail_url = 'https://www.jky888.com/jkyun/oms/trade/list'
        headers = {'Host': 'www.jky888.com',
                            'Connection': 'keep-alive',
                            'Content-Length': '5053',
                            'Accept': 'text/plain, */*; q=0.01',
                            'ati': str(self.session.cookies.get("ati")),
                            'X-Requested-With': 'XMLHttpRequest',
                            'Authorization': f'Bearer {self.session.cookies.get("token")}',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Origin': 'https://www.jky888.com',
                            'Sec-Fetch-Site': 'same-origin',
                            'Sec-Fetch-Mode': 'cors',
                            'Sec-Fetch-Dest': 'empty',                  
                            'Referer': 'http://www.jky888.com/oms/order/order_queryv2.html?_t=986361&_winid=w5088',
                            # 可能要修改
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'}
        pg_idx = 0
        start_data = genarate_query(self, pg_idx, sdt, edt)
        total_num = self.session.post(page_info_url, data=start_data, headers=headers)
        total_num = int(total_num.json().get('result').get('data'))
        result = []
        while total_num>0:
            result.append(self.session.post(detail_url, data=genarate_query(self, pg_idx, sdt, edt), headers=headers, verify=False))
            total_num -= 1000
            pg_idx+=1
            time.sleep(2)
        result = [i.json().get('result').get('data') for i in result]
        result = reduce(lambda x,y: x+y, result)
        result = pd.DataFrame(result)
        return result
    
    
    @staticmethod
    def format_stock_in(df):
        df['inOutDate'][df['inOutDate'].notnull()] = df['inOutDate'][df['inOutDate'].notnull()].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x)/1000)))
        df['gmtCreate'][df['gmtCreate'].notnull()] = df['gmtCreate'][df['gmtCreate'].notnull()].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x)/1000)))
        df['applyDate'][df['applyDate'].notnull()] = df['applyDate'][df['applyDate'].notnull()].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x)/1000)))
        return df

    def get_sign(self, dic):
        sign = ''.join([
            ''.join(i) for i in sorted(list(dic.items()), key=lambda x: x[0])
            if (i[0] not in ['_', 'sign']) & (len(i[1]) > 0)
        ])
        sign = self.secret + sign + self.secret
        m = hashlib.md5()
        m.update(sign.encode())
        sign = m.hexdigest().upper()
        return sign

    @staticmethod
    def get_split(df, pieces=2000):
        result = []
        n = math.floor(len(df) / pieces)
        l = len(df) % pieces
        for i in range(n):
            result.append((i * pieces, (i + 1) * pieces))
        result.append((n * pieces, (n * pieces + l + 1)))
        return result

    @staticmethod
    def ha(userid, passwd, timestamp):
        m = hashlib.md5()
        m.update((userid + passwd).encode())
        s = m.hexdigest().upper()
        m = hashlib.md5()
        m.update((s + str(timestamp)).encode())
        return m.hexdigest().upper()

    @staticmethod
    def hashing(s):
        m = hashlib.md5()
        m.update(str(s).encode())
        return m.hexdigest()
    
    @staticmethod
    def getFirstAndLastDay(year,month):
        # 获取当前月的第一天的星期和当月总天数
        weekDay,monthCountDay = calendar.monthrange(year,month)
        # 获取当前月份第一天
        firstDay = datetime.date(year,month,day=1).strftime("%Y-%m-%d") + " 00:00:00"
        # 获取当前月份最后一天
        lastDay = datetime.date(year,month,day=monthCountDay).strftime("%Y-%m-%d") + " 23:59:59"
        # 返回第一天和最后一天
        return firstDay,lastDay


if __name__ == '__main__':
    j = Jky()
    j.login()
    jky_con = connect('JKY')
    year = 2021
    month = '05' 
    df1 = j.get_monthly_order_shipment_data(year,5)
    df1 = rename_format(df1,1)
    df1.to_excel(f'1.上月发货销售订单明细数据{month}.xlsx',index=None)
    print('导出1完成')

    # df1.to_sql("temp_jky2021_05_ordermx_yfh", jky_con,
    #                 index=False, if_exists='append', method='multi', chunksize=100)


    df2 = j.get_monthly_aftersale_shipment_data(year,5)
    df2 = rename_format(df2,2)
    df2.to_excel(f'2.售后发货明细账{month}.xlsx',index=None)
    print('导出2完成')
    
    df3 = j.get_monthly_aftersale_return_data(year,5)
    df3 = rename_format(df3,3)
    df3.to_excel(f'3.售后退货明细账{month}.xlsx',index=None)
    print('导出3完成')
    
    df4 = j.get_monthly_compensate_refund_return_data(year,5)
    if len(df4) > 0:
       df4.to_excel(f'4补偿退款明细账{month}',index=None)
    print('导出4完成')
    
    df5 = j.get_monthly_error_send_return_data(year,5)
    if len(df5) > 0:
        df5.to_excel(f'5.错漏补发明细账{month}',index=None)
    print('导出5完成')
    
    df6 = j.get_monthly_history_warehouse_sdt_data(year,5)
    df6 = rename_format(df6,6)
    df6.to_excel(f'6.期初库存数据{month}.xlsx',index=None)
    print('导出6完成')
    
    df7 = j.get_monthly_history_warehouse_edt_data(year,5)
    df7 = rename_format(df7,7)
    df7.to_excel(f'7.期末库存数据{month}.xlsx',index=None)
    print('导出7完成')
    
    df8 = j.get_monthly_in_out_warehouse_data(year,5)
    df8 = rename_format(df8,8)
    df8.to_excel(f'8.出入库明细账{month}.xlsx',index=None)
    print('导出8完成')
    
    df9 = j.get_monthly_order_shipment_lastmonth_data(year,4)
    df9 = rename_format(df9,9)
    df9.to_excel(f'9.之月份已发货未完成上月完成订单明细数据{month}.xlsx',index=None)
    print('导出9完成')
    
    df10 = j.get_monthly_order_shipment_way_data(year,5)
    df10 = rename_format(df10,10)
    df10.to_excel(f'10.当前已发货未完成订单明细数据{month}.xlsx',index=None)
    print('导出10完成')
    
    # df11 = j.get_monthly_cainiao_salesorder_data(2021,4)
    # df11.to_excel(f'销售单查询{month}.xlsx',index=None)
    
    print('全部导出完成')
    
    

    # j.get_goods_data()
    # j.get_stock_curr()
    # j.get_order_details_new()
    # conn = connect('bidata')
    # conn = conn.connect()
    # with conn.begin():
    #     conn.execute("exec Online_goods_daily_update")
    # j.get_zh()
    # j.stock_in_run()
    
    
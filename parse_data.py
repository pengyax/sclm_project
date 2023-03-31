from urllib import parse

a = '''timestamp=1622425461797&access_token=Bearer F184755D9EAB0B558EAC7937D74B17F1&appkey=jackyun_web_browser&sign=E3A6BE58B6E646D8ADCF4C9608A84968&inOutDateType=1&timeType=3&trDateStart=2021-05-01%2000%3A00%3A00&trDateEnd=2021-05-31%2023%3A59%3A59&getType=1&billNos=&warehouseId=&channelIds=&skuId=&goodsNo=&goodsName=&skuName=&skuBarcode=&brandId=&cateId=&inOutType=&detailStatus=&userName=&vendCustomerName=&applyCompanyId=&applyDepartId=&applyUserId=&inOutReason=&isHideWriteOffBill=0&ownerId=960025061393039872&ownerName=647102&pageIndex=0&pageSize=1000&sortField=&sortOrder=&cols=%5B%22vendCustomerName%22%2C%22billNo%22%2C%22sourceBillNo%22%2C%22deliveryNo%22%2C%22warehouseName%22%2C%22trDate%22%2C%22systemTrDate%22%2C%22inouttypeName%22%2C%22goodsdocNo%22%2C%22goodsNo%22%2C%22goodsName%22%2C%22skuName%22%2C%22skuBarcode%22%2C%22channelName%22%2C%22retailPrice%22%2C%22retailAmtIn%22%2C%22retailAmtOut%22%2C%22unitName%22%2C%22isCertified%22%2C%22inCuPrice%22%2C%22inQuantity%22%2C%22inCuValue%22%2C%22outCuPrice%22%2C%22outQuantity%22%2C%22outCuValue%22%2C%22companyName%22%2C%22userName%22%2C%22memo%22%2C%22applyMemo%22%2C%22rowRemark%22%2C%22comment%22%2C%22inOutReason%22%2C%22settFeeAmount%22%2C%22estCost%22%2C%22estTax%22%2C%22applyCompanyName%22%2C%22applyDepartName%22%2C%22applyUserName%22%2C%22cateName%22%2C%22brandName%22%2C%22goodsAlias%22%2C%22goodsNameEn%22%2C%22outBillNo%22%2C%22batchNo%22%2C%22serialNo%22%2C%22goodsField1%22%2C%22goodsField2%22%5D
'''
a = parse.unquote(a)
dic  = {k:v for k,v in [i.split('=') for i in a.split('&')]}
for i,j in dic.items():
   print(f"'{i}':'{j}'")




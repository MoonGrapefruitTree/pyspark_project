

def get_dws_sales_union(sparkOff):
    df = sparkOff.read.format('json').load("../data/DWD/dwd_sales_clean").\
        select("storeProvince","storeID","receivable","dateTS","payType")
    df.write.mode("overwrite").format("json").save("../data/DWS/dws_sales_union")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_dws_sales_union(sparkOff)
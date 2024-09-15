def get_dwd_sales_clean(sparkOff):
    df = sparkOff.read.format('json').load("../data/ODS/ods_sales_base").\
        dropna(thresh=1,subset=['storeProvince']).\
        filter("storeProvince != 'null'").\
        filter("receivable<10000")
    df.write.mode("overwrite").format("json").save("../data/DWD/dwd_sales_clean")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_dwd_sales_clean(sparkOff)
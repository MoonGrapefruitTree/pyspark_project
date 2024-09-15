
def get_ods_sales_base(sparkOff):
    df = sparkOff.read.format('json').load("../data/mini.json")
    df.write.mode("overwrite").format("json").save("../data/ODS/ods_sales_base")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_ods_sales_base(sparkOff)
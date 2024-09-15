from pyspark.sql import functions as F

def get_ads_province_sales_amount(sparkOff):
    df = sparkOff.read.format('json').load("../data/DWS/dws_sales_union")
    province_sales_amount_df = df.groupBy('storeProvince').sum('receivable').\
        withColumnRenamed("sum(receivable)","money").\
        withColumn("money",F.round("money",2)).\
        orderBy('money',ascending=False)


    province_sales_amount_df.write.mode("overwrite").format("json").save("../data/ADS/ads_province_sales_amount")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_ads_province_sales_amount(sparkOff)
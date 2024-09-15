import pyspark.sql.functions as F
def get_ads_province_avg_sales_amount(sparkOff):
    provinces_data = sparkOff.read.format('json').load("../data/DWS/dws_sales_union")
    ads_province_avg_sales_amount = provinces_data.groupby("storeProvince"). \
        avg("receivable").withColumnRenamed("avg(receivable)", "money"). \
        withColumn("money",F.round("money",2)). \
        orderBy("money",ascending=False)
    ads_province_avg_sales_amount.write.mode("overwrite").format("json").save("../data/ADS/ads_province_avg_sales_amount")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_ads_province_avg_sales_amount(sparkOff)
import pyspark.sql.functions as F
def get_ads_top3_province_sales_data(sparkOff):
    # 前三的省份
    top3_provinces = sparkOff.read.format('json').load("../data/ADS/ads_province_sales_amount").orderBy("money", ascending=False).limit(3).select("storeProvince").withColumnRenamed("storeProvince","storeProvince_top3")
    # top3_provinces.show()
    # 原始的数据
    df = sparkOff.read.format('json').load("../data/DWS/dws_sales_union")
    # join 后的数据
    # top3_provinces_joined = df.join(top3_provinces, on='storeProvince', how='left')
    top3_provinces_joined = df.join(top3_provinces, on=df["storeProvince"] == top3_provinces["storeProvince_top3"], how="inner").drop("storeProvince_top3")
    top3_provinces_joined.write.mode("overwrite").format("json").save("../data/ADS/ads_top3_province_sales_data")

def get_ads_top3_province_more_1000_shops(sparkOff):
    # 前三的省份的数据
    top3_provinces_data = sparkOff.read.format('json').load("../data/ADS/ads_top3_province_sales_data")
    ads_top3_province_more_1000_shops = top3_provinces_data.groupby("storeProvince","storeID",F.from_unixtime(top3_provinces_data["dateTS"].substr(0,10),"yyyy-MM-dd").alias("date")).\
        sum("receivable").withColumnRenamed("sum(receivable)","money").\
        filter("money>1000").\
        dropDuplicates(subset=["storeID"]).\
        groupby("storeProvince").count()
    ads_top3_province_more_1000_shops.write.mode("overwrite").format("json").save("../data/ADS/ads_top3_province_more_1000_shops")


def get_ads_province_more_100_shops(sparkOff):
    provinces_data = sparkOff.read.format('json').load("../data/DWS/dws_sales_union")
    ads_province_more_100_shops = provinces_data.groupby("storeProvince", "storeID",
                                                         F.from_unixtime(
        provinces_data["dateTS"].substr(0, 10), "yyyy-MM-dd").alias("date")). \
        sum("receivable").withColumnRenamed("sum(receivable)", "money"). \
        filter("money>100"). \
        dropDuplicates(subset=["storeID"]). \
        groupby("storeProvince").count().withColumnRenamed("count","cnt").orderBy("cnt",ascending=False)
    ads_province_more_100_shops.write.mode("overwrite").format("json").save("../data/ADS/ads_province_more_100_shops")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_ads_top3_province_sales_data(sparkOff)
    get_ads_top3_province_more_1000_shops(sparkOff)
    get_ads_province_more_100_shops(sparkOff)
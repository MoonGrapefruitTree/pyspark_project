import pyspark.sql.functions as F
from pyspark.sql.types import StringType
def get_ads_province_payType_proportion(sparkOff):
    provinces_data = sparkOff.read.format('json').load("../data/DWS/dws_sales_union")

    provinces_data.createTempView("provinces_pay")

    def udf_func(percent):
        return str(round(percent*100,2))+"%"

    my_udf = F.udf(udf_func, StringType())

    ads_province_payType_proportion = sparkOff.sql("""
    select storeProvince, payType,(count(payType)/total) as percent from 
    (select storeProvince, payType,count(1) over(partition by storeProvince) as total from provinces_pay) as sub 
    group by storeProvince,payType,total
    """).withColumn("percent", my_udf("percent"))

    ads_province_payType_proportion.write.mode("overwrite").format("json").save("../data/ADS/ads_province_payType_proportion")

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_ads_province_payType_proportion(sparkOff)
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = ['sans-serif']
plt.rcParams['font.sans-serif'] = ['SimHei']

def get_province_sales_amount_fig(sparkOff):
    df = sparkOff.read.format('json').load("../data/ADS/ads_province_sales_amount").orderBy("money",ascending=False)
    pandas_df = df.toPandas()
    # 使用 Seaborn 和 Matplotlib 绘制图表
    plt.figure(figsize=(8, 6))

    # 绘制条形图
    plt.bar(pandas_df["storeProvince"],pandas_df["money"]/10000)
    # 省份太长 旋转一下
    plt.xticks(rotation=30)
    # 添加标题和标签
    plt.title("不同省份的销售额")
    plt.xlabel("省份")
    plt.ylabel("销售额")
    # 数据标签
    for i, v in enumerate(pandas_df["money"].tolist()):
        plt.text(i,int(v/10000)+1.5,str(round(v/10000,2))+"万",ha='center')


    # 设置留白
    plt.subplots_adjust(left=0.18, bottom=0.15)
    # 自动微调
    plt.tight_layout()
    # 保存图像
    plt.savefig('../data/BI/province_sales_amount_fig.png')
    # 显示图表
    plt.show()

if __name__ == '__main__':
    from dataWarehouse.offLine.Sessions import sparkOff
    get_province_sales_amount_fig(sparkOff)
from utils.visualize import plot_continuous_contract, plot_time_series, plot_volume_split

if __name__ == "__main__":
    print("开始生成图像")
    print("绘制成交量切分套利图...")
    try:
        plot_volume_split()
        print("成交量切分套利图生成完成（barplot目录）")
        print("绘制品种连续合约套利图...")
        plot_continuous_contract()
        print("品种连续合约套利图生成完成（barplot目录）")
        print("绘制固定套利对时序分析图...")
        plot_time_series()
    except Exception as e:
        print("图像生成失败")
        print(e)

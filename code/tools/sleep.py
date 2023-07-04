import random
import time

def sleep():
    mu = 2  # 平均值
    sigma = 0.5  # 标准差
    # 正态分布
    sleep_time = random.gauss(mu, sigma)
    sleep_time = max(1, min(3, sleep_time))  # 将休眠时间限制在1到3之间

    time.sleep(sleep_time)
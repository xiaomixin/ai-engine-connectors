## PEPE · Spoofing Scenario
```
t0: PEPE 买盘前 5 档深度突然放大（+300%）
t0 ~ t0+3s: 几乎没有主动买成交
t0+3s: 买盘深度迅速回落至原水平
t0+5s: 价格未被支撑 / 回落
```

F1：深度突增（Depth Surge）
F2：快速消失（Vanish Ratio）
F3：成交跟随不足（Execution Ratio）


深度（Depth） 指的是：
在某一侧盘口中，最接近成交价的前 K 个价位所提供的可成交数量总和。
选择 K=5 的原因：
Spoofing 行为主要集中在近 BBO 区域
前 5 档最能反映交易者“感知到的流动性”
比 K=1 稳定、比 K=20 更聚焦
在工程与行为上都是行业常用折中
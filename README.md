# Guangxi Real Estate Crawler

> 面向课程作业/学习爬虫的广西 14 个地级市基础房价数据采集示例  
>
> 平台：安居客（仅限公开展示页面；请遵守目标网站 ToS 与法律法规）  
>
> 代码作者：@lwzkdzz  (power by gpt)  
>
> License：MIT

---

## 目录
- [Guangxi Real Estate Crawler (安居客广西多城市二手房/新房采集脚本)](#guangxi-real-estate-crawler-安居客广西多城市二手房新房采集脚本)
  - [目录](#目录)
  - [项目结构](#项目结构)
  - [快速开始](#快速开始)
  - [命令行参数说明](#命令行参数说明)
  - [字段与数据字典](#字段与数据字典)
  - [故障排查与调优](#故障排查与调优)
  - [合规与使用限制](#合规与使用限制)
  - [致谢](#致谢)
  - [License (MIT)](#license-mit)
  - [免责声明](#免责声明)

## 项目结构
```
.
├─ guangxi_real_estate_crawler.py   # 主脚本（async + Playwright）
├─ README.md                        # 说明文档
├─ requirements.txt                 # 依赖列表
└─ data_city/                       # 输出目录（自动创建）
   ├─ nanning/
   │   ├─ nanning_esf_resume.json
   │   ├─ nanning_esf.xlsx
   │   └─ nanning_new.xlsx
   ├─ liuzhou/...
   └─ guangxi_merged.xlsx
```

## 快速开始

1. 冒烟测试：
   ```
   python guangxi_real_estate_crawler.py ^
     --city 南宁 ^
     --ip_mode header ^
     --target_count 30 ^
     --phase_a_quota 30 ^
     --phase_a_rounds 2 ^
     --min_per_district 0 ^
     --max_pages 2 ^
     --detail_sample_min 0.40 ^
     --detail_sample_max 0.50 ^
     --page_strategy hybrid ^
     --hybrid_random_pages 1 ^
     --deep_overlap_threshold 0.86 ^
     --deep_overlap_window 5 ^
     --deep_min_page_for_check 6 ^
     --rpm_soft_cap 16 ^
     --rpm_hard_cap 22 ^
     --detail_delay_min 5.0 ^
     --detail_delay_max 10.5 ^
     --page_delay_min 28 ^
     --page_delay_max 52 ^
     --adaptive_decay 0.985 ^
     --resume 1
   
   ```

2. 单城市推荐：
   ```
   python guangxi_real_estate_crawler.py ^
     --city 南宁 ^
     --ip_mode header ^
     --target_count 200 ^
     --phase_a_quota 100 ^
     --phase_a_rounds 2 ^
     --min_per_district 12 ^
     --max_pages 4 ^
     --detail_sample_min 0.50 ^
     --detail_sample_max 0.62 ^
     --page_strategy hybrid ^
     --hybrid_random_pages 2 ^
     --deep_overlap_threshold 0.87 ^
     --deep_overlap_window 5 ^
     --deep_min_page_for_check 7 ^
     --rpm_soft_cap 18 ^
     --rpm_hard_cap 26 ^
     --detail_delay_min 5.2 ^
     --detail_delay_max 10.8 ^
     --page_delay_min 32 ^
     --page_delay_max 58 ^
     --adaptive_decay 0.985 ^
     --resume 1
   
   ```

3. 作业快速版本：
   ```
   python guangxi_real_estate_crawler.py --guangxi_batch 1
   ```

---

## 命令行参数说明
| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| --cities | str | all | all 或 逗号分隔城市名 |
| --modes | str | esf | 可取 esf,new 或组合 |
| --per_city | int | 300 | 每城市二手房目标条数 |
| --max_pages | int | 120 | 翻页最大值（防止过深） |
| --new_limit | int | 150 | 新房楼盘采集上限 |
| --resume | int | 1 | 1=启用断点续爬缓存 |
| --out_dir | str | data_city | 输出根目录 |
| --merge | int | 1 | 1=结束后合并总表 |

逻辑说明：
- 二手房：逐页抓取，直到达到 per_city 或翻页到 max_pages。
- 新房：不强制目标量，抓到 new_limit 或翻页上限结束。

---

## 字段与数据字典
| 字段 | 说明 | 适用模式 | 备注 |
|------|------|----------|------|
| 平台 | 来源平台 | 全部 | 当前固定“安居客” |
| 类型 | 二手房/新房 | 新房为“新房” | 二手房缺省可为空 |
| 城市 | 地级市中文名 | 全部 | 如“南宁” |
| 行政区 | 市内区县 | 部分 | 正则启发式识别，可能为空 |
| 房源标题 | 房源详情页标题 | 二手房 |  |
| 楼盘名称 | 楼盘名 | 新房 |  |
| 户型 | 如 3室2厅 | 二手房 | 正则提取 |
| 户型样本 | 新房户型列表样本 | 新房 | 去重后前若干项 |
| 面积(㎡) | 建筑/套内面积 | 二手房 | 正则提取首个匹配 |
| 面积区间(㎡) | 面积区间 | 新房 | 可能为“88-120” |
| 单价(元/㎡) | 单价 | 全部 | 新房为均价/参考价 |
| 总价(万) | 总价 | 二手房 | 解析文本中的“xx万” |
| 建成年代 | 年份 | 二手房 | 正则匹配“XXXX年建” |
| 抓取日期 | YYYY-MM-DD | 全部 | 运行当天 |
| 详情URL | 详情页链接 | 全部 | 用于去重 & 溯源 |

数据清洗建议：
- 缺失行政区可通过 URL 或标题二次回填（未内置）
- 户型、面积异常值需人工过滤（如超大/超小）
- 新房户型样本建议展开为多行或 JSON 列

## 故障排查与调优
| 现象 | 可能原因 | 建议 |
|------|----------|------|
| 403/429 增多 | 频率过高 | 增加 sleep / 减少城市批量 / 使用代理池 |
| 数据列大量空 | 正则失效或结构变化 | 打印详情 HTML 重新分析; 调整正则 |
| 行政区缺失多 | 面包屑结构变化 | 增加更多行政区关键字或基于 URL 模式解析 |
| 合并表缺失部分城市 | 某城市采集失败或未生成文件 | 查看日志/单城市目录 |
| resume 不生效 | JSON 损坏 | 删除 resume json 重新跑 |
| 系统检测到您正在使用网页抓取工具访问安居客网站，请卸载删除后访问 | 被风控了 | 无 |

---

## 合规与使用限制
- 本项目仅供学习、教学、课程作业演示
- 请严格遵守目标网站的 服务条款 (Terms of Service) 与 robots 协议
- 不要在未授权场景中进行高频、大规模、商业化抓取
- 对任何数据的再分发请谨慎评估版权与合规风险
- 维护者不对因不当使用造成的合规或法律风险负责

---

## 致谢
- Requests / BeautifulSoup / Pandas / Playwright 开源社区
- fake-useragent 提供 UA 随机
- 使用者的反馈将帮助改进教学与案例质量
- 感谢安居客不强行跳登录界面

---

## License (MIT)
```
MIT License

Copyright (c) 2025 ...

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 免责声明
本项目不保证采集结果的准确性、完整性、时效性。使用者自行承担使用风险。

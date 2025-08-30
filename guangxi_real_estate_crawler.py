# -*- coding: utf-8 -*-
"""
面向作业需求的批量爬虫

平台: 安居客 (二手房,新房有bug)
技术栈: requests + BeautifulSoup（实测秒封）
实际需要基于Playwright (Chromium) + asyncio + BeautifulSoup

注意:
  - 仅用于作业/学习，请遵守目标站 ToS / robots。不要高频、不要商业用途。
  - 安居客页面结构若调整需更新解析逻辑。

Author: lwzkdzz
power by gpt
"""

import asyncio
import random
import argparse
import logging
import re
import ipaddress
import time
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from collections import deque

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =============== 城市映射 (含广西) ===============
CITY_MAP = {
    '北京':'beijing','bj':'beijing',
    '上海':'shanghai','sh':'shanghai',
    '广州':'guangzhou','gz':'guangzhou',
    '深圳':'shenzhen','sz':'shenzhen',
    '杭州':'hangzhou','hz':'hangzhou',
    '南京':'nanjing','nj':'nanjing',
    '武汉':'wuhan','wh':'wuhan',
    '成都':'chengdu','cd':'chengdu',
    '重庆':'chongqing','cq':'chongqing',
    '西安':'xian','xa':'xian',
    '天津':'tianjin','tj':'tianjin',
    '苏州':'suzhou','su':'suzhou',
    '青岛':'qingdao','qd':'qingdao',
    '大连':'dalian','dl':'dalian',
    '厦门':'xiamen','xm':'xiamen',
    '长沙':'changsha','cs':'changsha',
    '福州':'fuzhou','fz':'fuzhou',
    '郑州':'zhengzhou','zz':'zhengzhou',
    '济南':'jinan','jn':'jinan',
    '石家庄':'shijiazhuang','sjz':'shijiazhuang',
    '合肥':'hefei','hf':'hefei',
    '昆明':'kunming','km':'kunming',
    '太原':'taiyuan','ty':'taiyuan',
    '南昌':'nanchang','nc':'nanchang',
    '贵阳':'guiyang','gy':'guiyang',
    '乌鲁木齐':'wulumuqi','wlmq':'wulumuqi',
    '兰州':'lanzhou','lz':'lanzhou',
    '海口':'haikou','hk':'haikou',
    '银川':'yinchuan','yc':'yinchuan',
    '西宁':'xining','xn':'xining',
    '呼和浩特':'huhehaote','hhht':'huhehaote',
    # 广西
    '南宁':'nanning','柳州':'liuzhou','桂林':'guilin','梧州':'wuzhou',
    '北海':'beihai','防城港':'fangchenggang','钦州':'qinzhou','贵港':'guigang',
    '玉林':'yulin','百色':'baise','贺州':'hezhou','河池':'hechi',
    '来宾':'laibin','崇左':'chongzuo'
}
GUANGXI_CITIES = ["南宁","柳州","桂林","梧州","北海","防城港","钦州","贵港","玉林","百色","贺州","河池","来宾","崇左"]

# =============== 中国公网段 (示例) ===============
CN_CIDR_STRINGS = [
    "36.96.0.0/11","39.128.0.0/10","42.80.0.0/13","58.16.0.0/13","59.32.0.0/13",
    "60.160.0.0/11","61.128.0.0/10","101.64.0.0/11","111.192.0.0/10","112.64.0.0/11",
    "113.64.0.0/11","114.96.0.0/11","115.192.0.0/11","116.192.0.0/11","117.128.0.0/10",
    "118.112.0.0/12","119.32.0.0/12","119.128.0.0/10","120.192.0.0/10","121.32.0.0/13",
    "121.224.0.0/12","123.96.0.0/12","124.64.0.0/11","125.64.0.0/11","139.128.0.0/10",
    "140.240.0.0/12","171.8.0.0/13","171.104.0.0/13","175.0.0.0/12","182.80.0.0/12",
    "183.64.0.0/11","211.96.0.0/12","218.0.0.0/11","219.128.0.0/12","223.64.0.0/11"
]
CN_CIDRS = [ipaddress.ip_network(c) for c in CN_CIDR_STRINGS]

def generate_cn_ip() -> str:
    net = random.choice(CN_CIDRS)
    if net.num_addresses <= 4:
        return str(net.network_address + 1)
    rand_offset = random.randint(1, net.num_addresses - 2)
    return str(net.network_address + rand_offset)

def sanitize_text(t: str) -> str:
    if not t:
        return ''
    return re.sub(r'\s+', ' ', t).strip()

# =============== 速率限制 / 自适应 ===============
class RateLimiter:
    def __init__(self, rpm_soft: int, rpm_hard: int):
        self.rpm_soft = rpm_soft
        self.rpm_hard = rpm_hard
        self.events = deque()
    async def acquire(self):
        now = time.time()
        while self.events and now - self.events[0] > 60:
            self.events.popleft()
        cur = len(self.events)
        if cur >= self.rpm_hard:
            while True:
                await asyncio.sleep(2)
                now = time.time()
                while self.events and now - self.events[0] > 60:
                    self.events.popleft()
                if len(self.events) < self.rpm_soft:
                    break
        elif cur >= self.rpm_soft:
            await asyncio.sleep(random.uniform(3, 6))
        self.events.append(time.time())

class AdaptiveDelay:
    def __init__(self, page_min, page_max, detail_min, detail_max, adaptive_decay=0.98):
        self.page_min = page_min
        self.page_max = page_max
        self.detail_min = detail_min
        self.detail_max = detail_max
        self.factor = 1.0
        self.error_score = 0.0
        self.adaptive_decay = adaptive_decay
    def register_success(self):
        self.error_score = max(0.0, self.error_score - 0.5)
        self.factor = max(0.8, self.factor * self.adaptive_decay)
    def register_error(self, weight: float = 1.0):
        self.error_score += weight
        self.factor = min(3.0, 1.0 + math.log1p(self.error_score) / 1.5)
    def page_delay(self) -> float:
        base = random.uniform(self.page_min, self.page_max)
        tail = random.lognormvariate(0, 0.35)
        return max(8.0, (base + tail) * self.factor)
    def detail_delay(self) -> float:
        base = random.uniform(self.detail_min, self.detail_max)
        tail = random.lognormvariate(0, 0.25)
        return max(3.5, (base + 0.6 * tail) * min(self.factor, 2.5))

# =============== 页数 / 计划 / 抽样 ===============
def parse_total_pages(html: str) -> int:
    if not html:
        return 1
    cand = re.findall(r'/p(\d+)/', html)
    nums = [int(x) for x in cand if x.isdigit()]
    if nums:
        return max(nums)
    cand2 = re.findall(r'>(\d{1,3})<', html)
    nums2 = [int(x) for x in cand2 if x.isdigit() and int(x) < 1000]
    return max(nums2) if nums2 else 1

def build_page_plan(total_pages: int, strategy: str, limit: int, hybrid_random_pages: int) -> List[int]:
    if total_pages <= 1:
        return [1]
    limit = min(limit, total_pages)
    if strategy == "sequential":
        return list(range(1, limit + 1))
    if strategy == "random":
        pages = list(range(1, total_pages + 1))
        random.shuffle(pages)
        return sorted(pages[:limit])
    # hybrid
    pages = {1}
    rest = list(range(2, total_pages + 1))
    random.shuffle(rest)
    for p in rest[:hybrid_random_pages]:
        pages.add(p)
    for p in rest[hybrid_random_pages:]:
        if len(pages) >= limit:
            break
        pages.add(p)
    return sorted(pages)

def sample_details(links: List[str], sample_min: float, sample_max: float) -> List[str]:
    if not links:
        return []
    ratio = random.uniform(sample_min, sample_max)
    k = max(1, int(len(links) * ratio))
    random.shuffle(links)
    return links[:k]

# =============== 代理管理 ===============
class EnhancedProxyRecord:
    __slots__ = ("proxy","latency","success","fail","alive","last_used")
    def __init__(self, proxy: str, latency: float):
        self.proxy = proxy
        self.latency = latency
        self.success = 0
        self.fail = 0
        self.alive = True
        self.last_used = 0.0

class EnhancedProxyManager:
    def __init__(self, proxy_file: Optional[str], max_test: int = 150, test_url: Optional[str] = None,
                 timeout: float = 6.0, latency_limit: float = 4.0, max_workers: int = 40):
        self.proxy_file = proxy_file
        self.max_test = max_test
        self.test_url = test_url or "https://www.baidu.com"
        self.timeout = timeout
        self.latency_limit = latency_limit
        self.max_workers = max_workers
        self.records: List[EnhancedProxyRecord] = []
        self._rr_index = 0
        self._loaded_raw: List[str] = []
        self._load_file()
    def _load_file(self):
        if not self.proxy_file:
            logging.info("未提供代理文件，直连/伪造头模式")
            return
        path = Path(self.proxy_file)
        if not path.exists():
            logging.warning(f"代理文件不存在: {path}")
            return
        raw = []
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            if "://" not in line: line = "http://" + line
            raw.append(line)
        seen=set(); unique=[]
        for p in raw:
            if p not in seen:
                seen.add(p); unique.append(p)
        random.shuffle(unique)
        if self.max_test and len(unique) > self.max_test:
            unique = unique[:self.max_test]
        self._loaded_raw = unique
        logging.info(f"原始代理读取 {len(unique)} 条，开始健康检测...")
    def _test_one(self, proxy: str) -> Optional[EnhancedProxyRecord]:
        import requests
        try:
            start = time.time()
            r = requests.get(self.test_url, proxies={"http": proxy,"https": proxy}, timeout=self.timeout)
            dt = time.time() - start
            if r.status_code == 200 and dt <= self.latency_limit:
                return EnhancedProxyRecord(proxy, dt)
        except Exception:
            return None
        return None
    def warm_up(self):
        if not self._loaded_raw: return
        goods=[]
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures={ex.submit(self._test_one, p): p for p in self._loaded_raw}
            for fut in as_completed(futures):
                rec=fut.result()
                if rec: goods.append(rec)
        goods.sort(key=lambda r: r.latency)
        self.records=goods
        logging.info(f"可用代理 {len(goods)} 条 (延迟<= {self.latency_limit}s)")
        for r in goods[:8]:
            logging.info(f"  {r.proxy} {r.latency:.2f}s")
    def has_proxy(self)->bool:
        return any(r.alive for r in self.records)
    def pick(self)->Optional[EnhancedProxyRecord]:
        alive=[r for r in self.records if r.alive]
        if not alive: return None
        alive.sort(key=lambda r:(r.fail,r.success,r.latency))
        rec=alive[self._rr_index % len(alive)]
        self._rr_index+=1
        rec.last_used=time.time()
        return rec
    def report(self, rec: EnhancedProxyRecord, success: bool):
        if success:
            rec.success+=1
            rec.fail=0
        else:
            rec.fail+=1
            if rec.fail>=3:
                rec.alive=False
    def penalize_and_rotate(self, current: Optional[EnhancedProxyRecord]):
        if current:
            current.fail+=1
            if current.fail>=2:
                current.alive=False
        return self.pick()

# =============== 大延迟控制 ===============
class DelayController:
    def __init__(self, page_delay_min: int, page_delay_max: int,
                 long_pause_every: int, long_pause_min: int, long_pause_max: int):
        self.page_delay_min = page_delay_min
        self.page_delay_max = page_delay_max
        self.long_pause_every = long_pause_every
        self.long_pause_min = long_pause_min
        self.long_pause_max = long_pause_max
        self.page_counter = 0
        self.backoff_429 = 60
        self.backoff_max = 300
    async def page_delay(self):
        base = random.uniform(self.page_delay_min, self.page_delay_max)
        jitter = random.uniform(-1,1)
        d = max(5, base + jitter)
        logging.info(f"[Delay] 页级延迟 {d:.1f}s")
        await asyncio.sleep(d)
        self.page_counter += 1
        if self.long_pause_every>0 and self.page_counter % self.long_pause_every == 0:
            ld = random.uniform(self.long_pause_min, self.long_pause_max)
            logging.info(f"[Delay] 长暂停 {ld:.1f}s (模拟用户离开)")
            await asyncio.sleep(ld)
    async def captcha_backoff(self):
        t = random.uniform(180, 300)
        logging.warning(f"[Backoff] 验证码/403 触发，长休眠 {t:.1f}s")
        await asyncio.sleep(t)
    async def backoff_429_status(self):
        logging.warning(f"[Backoff] 429 休眠 {self.backoff_429}s")
        await asyncio.sleep(self.backoff_429)
        self.backoff_429 = min(self.backoff_429*2, self.backoff_max)
    def reset_429(self):
        self.backoff_429 = 60

# =============== 爬虫主体 ===============
class AnjukePlaywrightSpider:
    def __init__(
        self,
        city_input: str,
        max_pages: int = 2,
        headless: bool = True,
        slow_mo: int = 0,
        ip_mode: str = "header",
        proxy_manager: Optional[EnhancedProxyManager] = None,
        rotate_proxy_every: int = 0,
        target_count: int = 300,
        page_delay_min: int = 30,
        page_delay_max: int = 56,
        long_pause_every: int = 5,
        long_pause_min: int = 90,
        long_pause_max: int = 120,
        max_use_per_proxy: int = 8,
        proxy_fail_threshold: int = 2,
        page_strategy: str = "hybrid",
        detail_sample_min: float = 0.55,
        detail_sample_max: float = 0.85,
        detail_delay_min: float = 4.5,
        detail_delay_max: float = 9.0,
        rpm_soft_cap: int = 22,
        rpm_hard_cap: int = 28,
        adaptive_decay: float = 0.98,
        phase_a_quota: int = 100,
        min_per_district: int = 15,
        hybrid_random_pages: int = 2,
        resume_enabled: bool = False,
        resume_dir: str = "resume_data",
        resume_flush_interval: int = 10,
        # 新增广度与深页检测参数
        phase_a_rounds: int = 2,
        deep_overlap_threshold: float = 0.85,
        deep_overlap_window: int = 5,
        deep_min_page_for_check: int = 5
    ):
        self.city_input = city_input
        self.city_pinyin = CITY_MAP.get(city_input.lower(), CITY_MAP.get(city_input, city_input.lower()))
        self.city_display = city_input
        self.base_url = f"https://{self.city_pinyin}.anjuke.com"
        self.max_pages = max_pages
        self.headless = headless
        self.slow_mo = slow_mo
        self.ip_mode = ip_mode
        self.proxy_manager = proxy_manager
        self.rotate_proxy_every = rotate_proxy_every
        self.current_proxy_rec: Optional[EnhancedProxyRecord] = None
        self.current_proxy_success_pages = 0
        self.current_proxy_fail_pages = 0
        self.max_use_per_proxy = max_use_per_proxy
        self.proxy_fail_threshold = proxy_fail_threshold

        self.target_count = target_count
        self.total_pages_crawled = 0
        self.delay_ctrl = DelayController(page_delay_min, page_delay_max,
                                          long_pause_every, long_pause_min, long_pause_max)

        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.house_data: List[Dict[str, Any]] = []
        self._pw = None
        self._district_count = 0

        # 策略
        self.page_strategy = page_strategy
        self.detail_sample_min = detail_sample_min
        self.detail_sample_max = detail_sample_max
        self.detail_delay_min = detail_delay_min
        self.detail_delay_max = detail_delay_max
        self.rpm_soft_cap = rpm_soft_cap
        self.rpm_hard_cap = rpm_hard_cap
        self.adaptive_decay = adaptive_decay
        self.phase_a_quota = phase_a_quota
        self.min_per_district = min_per_district
        self.hybrid_random_pages = hybrid_random_pages
        self.per_district_counts: Dict[str, int] = {}
        self.rate_limiter = RateLimiter(rpm_soft_cap, rpm_hard_cap)
        self.adaptive_delay = AdaptiveDelay(page_delay_min, page_delay_max,
                                            detail_delay_min, detail_delay_max,
                                            adaptive_decay)

        # 断点续爬
        self.resume_enabled = resume_enabled
        self.resume_dir = Path(resume_dir)
        self.resume_dir.mkdir(parents=True, exist_ok=True)
        self.resume_path = self.resume_dir / f"{self.city_pinyin}_data.json"
        self.resume_flush_interval = resume_flush_interval
        self.visited_urls = set()

        # 深页重复检测
        self.deep_overlap_threshold = deep_overlap_threshold
        self.deep_overlap_window = deep_overlap_window
        self.deep_min_page_for_check = deep_min_page_for_check
        # recent_page_links[district] = deque([set(ids), ...], maxlen=window)
        self.recent_page_links: Dict[str, deque] = {}
        # 记录有效最大页（截断后设定）
        self.effective_max_page: Dict[str, int] = {}
        # Phase A 轮数配置
        self.phase_a_rounds = max(1, phase_a_rounds)
        # Phase A 已使用的页记录 (避免重复抓同一页)
        self.phase_a_used_pages: Dict[str, set] = {}

    # ---------- 断点续爬 ----------
    def load_resume(self):
        if not self.resume_enabled: return
        if self.resume_path.exists():
            try:
                data = json.loads(self.resume_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    self.house_data = data
                    for d in data:
                        url = d.get("url") or d.get("详情URL") or d.get("链接")
                        if url:
                            self.visited_urls.add(url)
                    for d in data:
                        dz = d.get("区域")
                        if dz:
                            self.per_district_counts[dz] = self.per_district_counts.get(dz, 0) + 1
                    logging.info(f"[Resume] 载入历史 {len(self.house_data)} 条, 已访问 {len(self.visited_urls)}")
            except Exception as e:
                logging.warning(f"[Resume] 读取失败: {e}")

    def flush_resume(self, force=False):
        if not self.resume_enabled: return
        if (not force) and (len(self.house_data) % self.resume_flush_interval != 0):
            return
        try:
            self.resume_path.write_text(json.dumps(self.house_data, ensure_ascii=False, indent=2), encoding="utf-8")
            logging.info(f"[Resume] 写入 {self.resume_path} (共 {len(self.house_data)} )")
        except Exception as e:
            logging.warning(f"[Resume] 写盘失败: {e}")

    # ---------- 浏览器 ----------
    async def init_browser(self, new_proxy: Optional[str] = None):
        if self._pw is None:
            self._pw = await async_playwright().start()
        launch_kwargs = {
            "headless": self.headless,
            "slow_mo": self.slow_mo,
            "args": [
                "--no-sandbox","--disable-setuid-sandbox","--disable-blink-features=AutomationControlled",
                "--disable-infobars","--disable-gpu","--disable-dev-shm-usage","--window-size=1600,900"
            ]
        }
        if new_proxy:
            launch_kwargs["proxy"] = {"server": new_proxy}
            logging.info(f"[Browser] 使用代理启动: {new_proxy}")
        ua_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        ]
        ua = random.choice(ua_pool)
        self.browser = await self._pw.chromium.launch(**launch_kwargs)
        self.context = await self.browser.new_context(
            viewport={"width":1600,"height":900},
            user_agent=ua,
            locale="zh-CN",
            extra_http_headers={"Accept-Language":"zh-CN,zh;q=0.9"}
        )
        async def route_handler(route, request):
            if request.resource_type in ("image","media","font"):
                await route.abort(); return
            headers = dict(request.headers)
            if self.ip_mode in ("header","both"):
                fake_ip = generate_cn_ip()
                headers["X-Forwarded-For"] = fake_ip
                headers["X-Real-IP"] = fake_ip
            await route.continue_(headers=headers)
        await self.context.route("**/*", route_handler)

    async def close_browser(self):
        if self.context:
            await self.context.close(); self.context=None
        if self.browser:
            await self.browser.close(); self.browser=None

    async def restart_browser_with_proxy(self, reason: str):
        await self.close_browser()
        proxy_to_use=None
        if self.ip_mode in ("proxy","both") and self.proxy_manager and self.proxy_manager.has_proxy():
            if not self.current_proxy_rec or not self.current_proxy_rec.alive:
                self.current_proxy_rec = self.proxy_manager.pick()
            proxy_to_use = self.current_proxy_rec.proxy if self.current_proxy_rec else None
        logging.info(f"[Proxy] 浏览器重启, 原因: {reason} 代理: {proxy_to_use}")
        await self.init_browser(new_proxy=proxy_to_use)
        self.current_proxy_success_pages=0
        self.current_proxy_fail_pages=0

    async def ensure_browser_started(self):
        if self.browser is None:
            if self.ip_mode in ("proxy","both") and self.proxy_manager and self.proxy_manager.has_proxy():
                if not self.current_proxy_rec:
                    self.current_proxy_rec = self.proxy_manager.pick()
                proxy_to_use = self.current_proxy_rec.proxy if self.current_proxy_rec else None
                await self.init_browser(new_proxy=proxy_to_use)
            else:
                await self.init_browser()

    async def fetch_html(self, url: str, wait_selector: str | None = None, timeout: int = 30000) -> str | None:
        await self.ensure_browser_started()
        page: Page = await self.context.new_page()
        try:
            if self.rate_limiter:
                await self.rate_limiter.acquire()
            logging.info(f"访问: {url}")
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            if not resp or resp.status != 200:
                logging.warning(f"状态码异常: {resp.status if resp else 'No Response'} - {url}")
            await page.wait_for_timeout(random.randint(800,1500))
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=5000)
                except:
                    pass
            html = await page.content()
            return html
        except Exception as e:
            logging.error(f"获取失败: {url} - {e}")
            return None
        finally:
            await page.close()

    # ---------- 解析 ----------
    def parse_districts(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        districts=[]
        span_list = soup.find_all("span")
        area_div=None
        for sp in span_list:
            if sanitize_text(sp.get_text()) == "二手房":
                parent=sp.parent
                for _ in range(6):
                    if not parent: break
                    ah = parent.find("div", class_="areaheight")
                    if ah:
                        area_div=ah; break
                    parent = parent.parent
                if area_div: break
        if not area_div:
            for cand in soup.find_all("div", class_="areaheight"):
                if cand.find("a", href=lambda h: h and "/sale/" in h):
                    area_div=cand; break
        if not area_div:
            logging.error("未找到二手房区域区块 areaheight")
            return districts
        for a in area_div.find_all("a", class_="jumplink"):
            href=a.get("href","")
            title=sanitize_text(a.get("title") or a.get_text())
            if "/sale/" in href and title and title!="其他":
                if not href.endswith("/"): href+="/"
                districts.append((title, href))
                logging.info(f"区域捕获: {title} -> {href}")
        logging.info(f"共解析出区域数: {len(districts)}")
        return districts

    def parse_list(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        links=[]
        for a in soup.find_all("a", class_="property-ex"):
            href=a.get("href","")
            if "/prop/view/" in href:
                links.append(href.split("?")[0])
        links=list(dict.fromkeys(links))
        logging.info(f"列表页提取房源数量: {len(links)}")
        return links

    def parse_detail(self, html: str, url: str):
        soup = BeautifulSoup(html, "html.parser")
        maininfo = soup.find("div", class_="maininfo")
        if not maininfo:
            logging.warning(f"未找到 maininfo: {url}")
            return None
        data={"url":url}
        price_wrap=maininfo.find("div", class_="maininfo-price")
        if price_wrap:
            num=price_wrap.find("span", class_="maininfo-price-num")
            unit=price_wrap.find("span", class_="maininfo-price-unit")
            if num and unit:
                data["总价"]=f"{sanitize_text(num.text)}{sanitize_text(unit.text)}"
            avg=price_wrap.find("div", class_="maininfo-avgprice-price")
            if avg: data["单价"]=sanitize_text(avg.text)
        model=maininfo.find("div", class_="maininfo-model")
        if model:
            for item in model.find_all("div", class_="maininfo-model-item"):
                strong=item.find("div", class_="maininfo-model-strong")
                weak=item.find("div", class_="maininfo-model-weak")
                stext=sanitize_text(strong.get_text()) if strong else ""
                wtext=sanitize_text(weak.get_text()) if weak else ""
                if not stext: continue
                if "室" in stext and "厅" in stext:
                    data["房型"]=stext
                    if wtext: data["楼层"]=wtext
                elif "㎡" in stext:
                    data["面积"]=stext
                    if wtext: data["装修"]=wtext
                else:
                    if any(d in stext for d in ["南","北","东","西"]):
                        data["朝向"]=stext
                        if wtext: data["建造年份"]=wtext
        tag_div=maininfo.find("div", class_="maininfo-tags")
        if tag_div:
            tags=[sanitize_text(x.get_text()) for x in tag_div.find_all("span") if sanitize_text(x.get_text())]
            if tags: data["标签"]=", ".join(tags)
        comm=maininfo.find("div", class_="maininfo-community")
        if comm:
            items=comm.find_all("div", class_="maininfo-community-item")
            for it in items:
                label=it.find("span", class_="maininfo-community-item-label")
                label_text=sanitize_text(label.get_text()) if label else ""
                if "所属小区" in label_text:
                    a=it.find("a")
                    if a: data["小区名称"]=sanitize_text(a.get_text())
                if "所属区域" in label_text:
                    als=it.find_all("a")
                    if als:
                        data["所属区域"]=" ".join(sanitize_text(a.get_text()) for a in als)
        broker=maininfo.find("div", class_="maininfo-broker")
        if broker:
            name=broker.find("div", class_="maininfo-broker-info-name")
            if name: data["经纪人"]=sanitize_text(name.get_text())
            score=broker.find("span", class_="maininfo-broker-info-score-num")
            if score: data["经纪人评分"]=sanitize_text(score.get_text())
            company=broker.find("a", class_="anchor")
            if company:
                comp_text=sanitize_text(company.get_text())
                if comp_text: data["经纪公司"]=comp_text
        return data

    def detect_captcha_or_block(self, html: Optional[str]) -> bool:
        if not html: return False
        low=html.lower()
        markers=["captcha","验证您","人机验证","访问验证","请点击按钮完成验证","安全检查"]
        return any(m.lower() in low for m in markers)

    # ---------- 深页重复检测 ----------
    def _init_recent_deque(self, district: str):
        if district not in self.recent_page_links:
            self.recent_page_links[district] = deque(maxlen=self.deep_overlap_window)

    def deep_page_overlap_trigger(self, district: str, page_num: int, new_ids: List[str]) -> bool:
        """
        返回 True 表示触发截断。
        """
        self._init_recent_deque(district)
        s_new = set(new_ids)
        if not s_new:
            return False
        suspicious = False
        for old_set in self.recent_page_links[district]:
            overlap = len(s_new & old_set) / max(1, len(s_new))
            if overlap >= self.deep_overlap_threshold and page_num >= self.deep_min_page_for_check:
                logging.warning(f"[DeepCheck] {district} 页{page_num} 与历史页重合率 {overlap:.2%} >= 阈值 {self.deep_overlap_threshold:.2f} -> 截断后续深页")
                suspicious = True
                break
        self.recent_page_links[district].append(s_new)
        if suspicious:
            self.effective_max_page[district] = page_num - 1
        return suspicious

    # ---------- Phase A 单页抓取（广度模式用） ----------
    async def crawl_district_single_page(self,
                                         district_name: str,
                                         district_url: str,
                                         page_num: int,
                                         cached_first_html: Optional[str] = None):
        if len(self.house_data) >= self.target_count:
            return
        if page_num == 1:
            list_html = cached_first_html
            if not list_html:
                list_html = await self.fetch_html(district_url, wait_selector="a.property-ex")
        else:
            page_url = district_url if district_url.endswith("/") else district_url + "/"
            page_url = f"{page_url}p{page_num}/"
            list_html = await self.fetch_html(page_url, wait_selector="a.property-ex")
        if not list_html:
            logging.warning(f"[PhaseA] 区 {district_name} 页{page_num} 获取失败")
            self.adaptive_delay.register_error(0.8)
            return
        if self.detect_captcha_or_block(list_html):
            logging.warning(f"[PhaseA] 区 {district_name} 页{page_num} 验证码/拦截")
            self.adaptive_delay.register_error(2.0)
            await self.delay_ctrl.captcha_backoff()
            return
        house_links = self.parse_list(list_html)
        if not house_links:
            logging.info(f"[PhaseA] 区 {district_name} 页{page_num} 无房源链接")
            return
        # 深页重合检测（page_num>1 适用）
        if page_num > 1:
            if self.deep_page_overlap_trigger(district_name, page_num, house_links):
                return
        sampled = sample_details(house_links, self.detail_sample_min, self.detail_sample_max)
        logging.info(f"[PhaseA] {district_name} 页{page_num} 原始 {len(house_links)} 抽样 {len(sampled)}")
        await self._process_detail_batch(district_name, sampled)

    # ---------- 处理详情批 ----------
    async def _process_detail_batch(self, district_name: str, sampled_links: List[str]):
        for idx, hlink in enumerate(sampled_links, 1):
            if len(self.house_data) >= self.target_count:
                break
            if hlink in self.visited_urls:
                continue
            ddelay = self.adaptive_delay.detail_delay()
            logging.info(f"详情 {idx}/{len(sampled_links)} 延迟 {ddelay:.1f}s -> {hlink} (factor={self.adaptive_delay.factor:.2f})")
            await asyncio.sleep(ddelay)
            detail_html = await self.fetch_html(hlink, wait_selector="div.maininfo")
            if not detail_html:
                self.adaptive_delay.register_error(0.6)
                continue
            if self.detect_captcha_or_block(detail_html):
                logging.warning("检测到验证码/拦截 (详情页)")
                self.adaptive_delay.register_error(2.5)
                await self.delay_ctrl.captcha_backoff()
                continue
            detail = self.parse_detail(detail_html, hlink)
            if detail:
                detail["城市"] = self.city_display
                detail["区域"] = district_name
                self.house_data.append(detail)
                self.visited_urls.add(hlink)
                self.adaptive_delay.register_success()
                if len(self.house_data) % 50 == 0:
                    logging.info(f"已收集 {len(self.house_data)} 条")
                self.flush_resume()

    # ---------- 区域抓取 (Phase B/C 深度使用) ----------
    async def crawl_district(self, district_name: str, district_url: str):
        logging.info(f"开始区域: {district_name} -> {district_url}")
        first_html = await self.fetch_html(district_url, wait_selector="a.property-ex")
        if not first_html:
            logging.warning(f"初始页失败: {district_url}")
            self.current_proxy_fail_pages += 1
            return
        if self.detect_captcha_or_block(first_html):
            logging.warning("初始页验证码/拦截")
            await self.delay_ctrl.captcha_backoff()
            await self.rotate_proxy_on_event("captcha_district_init")
            return
        total_pages = parse_total_pages(first_html)
        eff_max = self.effective_max_page.get(district_name, total_pages)
        total_pages = min(total_pages, eff_max)
        page_plan = build_page_plan(total_pages, self.page_strategy, self.max_pages, self.hybrid_random_pages)
        logging.info(f"[{district_name}] 总页数={total_pages} 计划={page_plan}")
        cached_first = {1: first_html}
        for page_num in page_plan:
            if len(self.house_data) >= self.target_count:
                logging.info("已达到 target_count，提前结束该区域。")
                break
            if district_name in self.effective_max_page and page_num > self.effective_max_page[district_name]:
                logging.info(f"[DeepCheck] 已截断 {district_name} 的最大页为 {self.effective_max_page[district_name]}，跳过页{page_num}")
                break
            if page_num == 1:
                list_html = cached_first[1]
                page_url = district_url
            else:
                page_url = district_url if district_url.endswith("/") else district_url + "/"
                page_url = f"{page_url}p{page_num}/"
                list_html = await self.fetch_html(page_url, wait_selector="a.property-ex")
            logging.info(f"[{district_name}] 页 {page_num} URL: {page_url}")
            if not list_html:
                logging.warning(f"列表页获取失败: {page_url}")
                self.current_proxy_fail_pages += 1
                self.adaptive_delay.register_error(1.2)
                if self.current_proxy_fail_pages >= self.proxy_fail_threshold:
                    await self.rotate_proxy_on_event("list_page_fail")
                continue
            if self.detect_captcha_or_block(list_html):
                logging.warning("检测到疑似验证码/拦截页 (列表页)")
                self.adaptive_delay.register_error(2.5)
                await self.delay_ctrl.captcha_backoff()
                await self.rotate_proxy_on_event("captcha_list")
                continue
            self.current_proxy_success_pages += 1
            self.current_proxy_fail_pages = 0
            house_links = self.parse_list(list_html)
            if not house_links:
                logging.info(f"无房源链接，跳过后续页: {page_url}")
                self.adaptive_delay.register_error(0.8)
                continue

            # 深页重复检测
            if page_num > 1:
                if self.deep_page_overlap_trigger(district_name, page_num, house_links):
                    logging.info(f"[DeepCheck] 触发后停止 {district_name} 的后续深页")
                    break

            sampled = sample_details(house_links, self.detail_sample_min, self.detail_sample_max)
            logging.info(f"[{district_name}] 页 {page_num} 原始 {len(house_links)} 抽样 {len(sampled)}")
            await self._process_detail_batch(district_name, sampled)

            self.total_pages_crawled += 1
            if len(self.house_data) >= self.target_count:
                break
            pdelay = self.adaptive_delay.page_delay()
            logging.info(f"[Delay] 页级延迟 {pdelay:.1f}s (factor={self.adaptive_delay.factor:.2f})")
            await asyncio.sleep(pdelay)
            if (self.ip_mode in ("proxy","both") and
                self.current_proxy_rec and
                self.max_use_per_proxy > 0 and
                self.current_proxy_success_pages >= self.max_use_per_proxy):
                await self.rotate_proxy_on_event("max_use_per_proxy")

        area_sleep = random.uniform(8, 12)
        logging.info(f"区域 {district_name} 完成，区域级休息 {area_sleep:.1f}s")
        await asyncio.sleep(area_sleep)
        self._district_count += 1
        got = sum(1 for d in self.house_data if d.get("区域") == district_name)
        self.per_district_counts[district_name] = got
        if self.rotate_proxy_every > 0 and self._district_count % self.rotate_proxy_every == 0:
            await self.rotate_proxy_on_event("legacy_rotate_every_area")

    # ---------- 代理切换 ----------
    async def rotate_proxy_on_event(self, reason: str):
        if self.ip_mode not in ("proxy","both"):
            logging.info(f"[Proxy] 非代理模式，忽略轮换 ({reason})")
            return
        if not self.proxy_manager or not self.proxy_manager.has_proxy():
            logging.info(f"[Proxy] 无可用代理可轮换 ({reason})")
            return
        if self.current_proxy_rec:
            self.proxy_manager.report(self.current_proxy_rec, False)
        self.current_proxy_rec = self.proxy_manager.penalize_and_rotate(self.current_proxy_rec)
        await self.restart_browser_with_proxy(reason)

    # ---------- Phase A 广度优先 ----------
    async def phase_a_breadth_cover(self, districts: List[Tuple[str,str]]):
        """
        多轮：
          Round0: 每区第一页
          Round1: 中部随机页 (total_pages>1)
          Round2(可选): 尾部随机页 (phase_a_rounds>=3)
        直到达到 phase_a_quota 或 target_count
        """
        target_a = min(self.phase_a_quota, self.target_count)
        if target_a <= 0:
            return
        # 先抓取各区第一页，记录总页数
        total_pages_map: Dict[str,int] = {}
        first_html_map: Dict[str,str] = {}
        random.shuffle(districts)
        logging.info(f"[Phase A] 广度 Round0 (第一页) 目标 {target_a}")
        for dname, durl in districts:
            if len(self.house_data) >= target_a:
                break
            # 抓第一页
            first_html = await self.fetch_html(durl, wait_selector="a.property-ex")
            if not first_html:
                logging.warning(f"[PhaseA] {dname} 首页失败")
                continue
            if self.detect_captcha_or_block(first_html):
                logging.warning(f"[PhaseA] {dname} 首页验证码/拦截")
                await self.delay_ctrl.captcha_backoff()
                continue
            tp = parse_total_pages(first_html)
            eff_max = self.effective_max_page.get(dname, tp)
            tp = min(tp, eff_max)
            total_pages_map[dname]=tp
            first_html_map[dname]=first_html
            self.phase_a_used_pages.setdefault(dname, set()).add(1)
            await self.crawl_district_single_page(dname, durl, 1, cached_first_html=first_html)
        logging.info(f"[Phase A] Round0 完成 {len(self.house_data)}/{target_a}")

        # 后续轮：中部 / 尾部
        if len(self.house_data) >= target_a:
            logging.info(f"[Phase A] 达到配额 {len(self.house_data)}/{target_a}，结束 Phase A")
            return

        for round_idx in range(1, self.phase_a_rounds):
            if len(self.house_data) >= target_a:
                break
            round_type = "middle" if round_idx == 1 else ("tail" if round_idx == 2 else f"extra{round_idx}")
            logging.info(f"[Phase A] 广度 Round{round_idx} ({round_type})")
            # shuffle 再次打乱
            shuffled = districts[:]
            random.shuffle(shuffled)
            for dname, durl in shuffled:
                if len(self.house_data) >= target_a:
                    break
                tp = total_pages_map.get(dname)
                if not tp or tp <= 1:
                    continue
                used = self.phase_a_used_pages.setdefault(dname, set())
                # 选择页面
                candidate = None
                if round_type == "middle":
                    low = max(2, tp//3)
                    high = max(2, tp-2)
                    if high < low: high = low
                    tries = 0
                    while tries < 6:
                        c = random.randint(low, high)
                        if c not in used:
                            candidate = c; break
                        tries += 1
                elif round_type == "tail":
                    low = max(2, tp-6)
                    low = min(low, tp)  # ensure <= tp
                    if low < 2: low = 2
                    tries = 0
                    while tries < 6:
                        c = random.randint(low, tp)
                        if c not in used:
                            candidate = c; break
                        tries += 1
                else:
                    # 额外轮：随机
                    tries = 0
                    while tries < 6:
                        c = random.randint(2, tp)
                        if c not in used:
                            candidate = c; break
                        tries += 1
                if not candidate:
                    continue
                used.add(candidate)
                await self.crawl_district_single_page(dname, durl, candidate)
            logging.info(f"[Phase A] Round{round_idx} 完成 {len(self.house_data)}/{target_a}")

        logging.info(f"[Phase A] 最终完成 {len(self.house_data)}/{target_a}")

    # ---------- 输出 ----------
    def save_excel(self):
        if not self.house_data:
            logging.warning("无数据可写入 Excel")
            return
        df = pd.DataFrame(self.house_data)
        columns_order = [
            "城市","区域","小区名称","房型","面积","总价","单价","朝向",
            "楼层","装修","建造年份","标签","所属区域","经纪人",
            "经纪人评分","经纪公司","url"
        ]
        existing_cols = [c for c in columns_order if c in df.columns]
        df = df[existing_cols]
        fname = f"anjuke_{self.city_pinyin}_esf.xlsx"
        try:
            df.to_excel(fname, index=False)
            logging.info(f"已保存 Excel: {fname} (共 {len(df)} 条)")
        except ModuleNotFoundError:
            csv_name = f"anjuke_{self.city_pinyin}_esf.csv"
            df.to_csv(csv_name, index=False, encoding="utf-8-sig")
            logging.info(f"openpyxl 不存在，已保存 CSV: {csv_name} (共 {len(df)} 条)")
        self.flush_resume(force=True)

    # ---------- 主流程 ----------
    async def run(self):
        logging.info(f"启动爬取: {self.city_display}({self.city_pinyin}) max_pages/区:{self.max_pages} 目标:{self.target_count} 策略:{self.page_strategy}")
        if self.city_pinyin == "kelamayi":
            logging.error("该城市已被禁用。")
            return
        if self.resume_enabled:
            self.load_resume()
            if len(self.house_data) >= self.target_count:
                logging.info("[Resume] 已达到目标，直接输出。")
                self.save_excel()
                return
        try:
            home_html = await self.fetch_html(self.base_url, wait_selector="div.areaheight")
            if not home_html:
                logging.error("首页获取失败，终止。")
                return
            if self.detect_captcha_or_block(home_html):
                logging.error("首页验证码/拦截，尝试更换代理")
                await self.delay_ctrl.captcha_backoff()
                await self.rotate_proxy_on_event("captcha_home")
                home_html = await self.fetch_html(self.base_url, wait_selector="div.areaheight")
                if not home_html or self.detect_captcha_or_block(home_html):
                    logging.error("首页二次仍失败，退出。")
                    return
            districts = self.parse_districts(home_html)
            if not districts:
                logging.error("未解析出区域，终止。")
                return

            # Phase A 广度
            await self.phase_a_breadth_cover(districts)
            if len(self.house_data) >= self.target_count:
                self.save_excel(); return

            # Phase B 补齐（保证每区最少）
            if self.min_per_district > 0:
                logging.info(f"[Phase B] 补齐阶段 (每区至少 {self.min_per_district})")
                for dname, durl in districts:
                    if len(self.house_data) >= self.target_count: break
                    got = self.per_district_counts.get(dname, 0)
                    if got < self.min_per_district:
                        # 仅抓少量页：max_pages=1 提升广度
                        prev = self.max_pages
                        self.max_pages = 1
                        await self.crawl_district(dname, durl)
                        self.max_pages = prev
                logging.info(f"[Phase B] 完成 -> {len(self.house_data)} 条")
                if len(self.house_data) >= self.target_count:
                    self.save_excel(); return

            # Phase C 深度产出
            logging.info("[Phase C] 产出阶段 (深度挖掘)")
            districts_sorted = sorted(districts, key=lambda x: self.per_district_counts.get(x[0],0))
            for dname, durl in districts_sorted:
                if len(self.house_data) >= self.target_count: break
                await self.crawl_district(dname, durl)
            self.save_excel()
        finally:
            await self.close_browser()
            if self._pw:
                await self._pw.stop()
            logging.info("任务结束。")

# =============== 参数解析 ===============
def parse_args():
    ap = argparse.ArgumentParser(description="安居客二手房 Playwright 爬虫 (教学/练习用)")
    ap.add_argument("--city", required=False)
    ap.add_argument("--max_pages", type=int, default=3)
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--slow_mo", type=int, default=0)
    ap.add_argument("--ip_mode", choices=["header","proxy","both"], default="header")
    ap.add_argument("--proxy_file", type=str, default="")
    ap.add_argument("--rotate_proxy_every", type=int, default=0)
    ap.add_argument("--target_count", type=int, default=300)
    ap.add_argument("--page_delay_min", type=int, default=30)
    ap.add_argument("--page_delay_max", type=int, default=56)
    ap.add_argument("--long_pause_every", type=int, default=5)
    ap.add_argument("--long_pause_min", type=int, default=90)
    ap.add_argument("--long_pause_max", type=int, default=120)
    ap.add_argument("--max_use_per_proxy", type=int, default=8)
    ap.add_argument("--proxy_fail_threshold", type=int, default=2)
    ap.add_argument("--proxy_health_test", type=int, default=150)
    ap.add_argument("--test_url", type=str, default="")
    ap.add_argument("--page_strategy", choices=["sequential","random","hybrid"], default="hybrid")
    ap.add_argument("--detail_sample_min", type=float, default=0.55)
    ap.add_argument("--detail_sample_max", type=float, default=0.85)
    ap.add_argument("--detail_delay_min", type=float, default=4.5)
    ap.add_argument("--detail_delay_max", type=float, default=9.0)
    ap.add_argument("--rpm_soft_cap", type=int, default=22)
    ap.add_argument("--rpm_hard_cap", type=int, default=28)
    ap.add_argument("--adaptive_decay", type=float, default=0.98)
    ap.add_argument("--phase_a_quota", type=int, default=100)
    ap.add_argument("--min_per_district", type=int, default=15)
    ap.add_argument("--hybrid_random_pages", type=int, default=2)
    ap.add_argument("--resume", type=int, default=0)
    ap.add_argument("--resume_dir", type=str, default="resume_data")
    ap.add_argument("--resume_flush_interval", type=int, default=10)
    ap.add_argument("--guangxi_batch", type=int, default=0)
    # 新增参数
    ap.add_argument("--phase_a_rounds", type=int, default=2, help="Phase A 广度轮数: 1=仅第一页;2=+中部;3=+尾部")
    ap.add_argument("--deep_overlap_threshold", type=float, default=0.85, help="深页重复率阈值")
    ap.add_argument("--deep_overlap_window", type=int, default=5, help="比较最近多少页")
    ap.add_argument("--deep_min_page_for_check", type=int, default=5, help="从第几页起才截断判定")
    return ap.parse_args()

# =============== 主入口 ===============
async def main():
    args = parse_args()
    if not args.guangxi_batch and not args.city:
        raise SystemExit("必须指定 --city 或 --guangxi_batch 1")

    proxy_manager=None
    if args.proxy_file:
        proxy_manager = EnhancedProxyManager(
            proxy_file=args.proxy_file,
            max_test=args.proxy_health_test,
            test_url=args.test_url or None
        )
        proxy_manager.warm_up()
        if not proxy_manager.has_proxy():
            logging.warning("无可用代理, 将退回 header/直连模式")

    async def run_one(city_name: str):
        spider = AnjukePlaywrightSpider(
            city_input=city_name,
            max_pages=args.max_pages,
            headless=bool(args.headless),
            slow_mo=args.slow_mo,
            ip_mode=args.ip_mode,
            proxy_manager=proxy_manager,
            rotate_proxy_every=args.rotate_proxy_every,
            target_count=args.target_count,
            page_delay_min=args.page_delay_min,
            page_delay_max=args.page_delay_max,
            long_pause_every=args.long_pause_every,
            long_pause_min=args.long_pause_min,
            long_pause_max=args.long_pause_max,
            max_use_per_proxy=args.max_use_per_proxy,
            proxy_fail_threshold=args.proxy_fail_threshold,
            page_strategy=args.page_strategy,
            detail_sample_min=args.detail_sample_min,
            detail_sample_max=args.detail_sample_max,
            detail_delay_min=args.detail_delay_min,
            detail_delay_max=args.detail_delay_max,
            rpm_soft_cap=args.rpm_soft_cap,
            rpm_hard_cap=args.rpm_hard_cap,
            adaptive_decay=args.adaptive_decay,
            phase_a_quota=args.phase_a_quota,
            min_per_district=args.min_per_district,
            hybrid_random_pages=args.hybrid_random_pages,
            resume_enabled=bool(args.resume),
            resume_dir=args.resume_dir,
            resume_flush_interval=args.resume_flush_interval,
            phase_a_rounds=args.phase_a_rounds,
            deep_overlap_threshold=args.deep_overlap_threshold,
            deep_overlap_window=args.deep_overlap_window,
            deep_min_page_for_check=args.deep_min_page_for_check
        )
        await spider.run()

    if args.guangxi_batch:
        for idx, city in enumerate(GUANGXI_CITIES, 1):
            logging.info(f"========== Guangxi {idx}/{len(GUANGXI_CITIES)} {city} ==========")
            await run_one(city)
            if idx < len(GUANGXI_CITIES):
                gap = random.uniform(60,120)
                logging.info(f"[Batch] 下一城市休息 {gap:.1f}s")
                await asyncio.sleep(gap)
    else:
        await run_one(args.city)

if __name__ == "__main__":
    asyncio.run(main())

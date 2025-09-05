#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import sqlite3
import re
import time
import random
import argparse
import os
import sys
import base64
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# 全局变量
BASE_URL = "https://m.dsq4d.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Referer": "https://m.dsq4d.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# 分类ID和名称映射
CATEGORIES = {
    1: "电影",
    2: "电视剧", 
    3: "动漫",
    4: "综艺",
    19: "大陆剧",
    20: "欧美剧", 
    21: "香港剧",
    22: "韩国剧",    
    23: "台湾剧",
    24: "日本剧", 
    25: "海外剧",
    26: "泰国剧",  
    27: "短剧"
}

# 数据库文件
DB_FILE = "dy.db"

class OptimizedDSQ4DCrawler:
    def __init__(self, test_mode=False, delay=0.1, max_workers=8, batch_size=50):
        """初始化优化爬虫"""
        self.test_mode = test_mode
        self.delay = delay
        self.max_workers = max_workers
        self.batch_size = batch_size
        
        # 创建优化的session
        self.session = self._create_optimized_session()
        
        # 数据库连接池
        self.db_lock = threading.Lock()
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        # 批量操作缓存
        self.movie_batch = []
        self.m3u8_batch = []
        self.batch_lock = threading.Lock()
        
        self._ensure_tables()
        
    def _create_optimized_session(self):
        """创建优化的HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        # 配置适配器
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20,
            pool_block=False
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)
        
        return session
    
    def _ensure_tables(self):
        """确保所需的数据库表已创建"""
        tables = ["dy", "m3u8", "crawl_progress"]
        for table in tables:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if not cursor.fetchone():
                print(f"表 {table} 不存在，请先运行 init_db.py 初始化数据库")
                sys.exit(1)
            cursor.close()
    
    def _get_with_retry(self, url, timeout=5):
        """发送GET请求，带优化的重试机制"""
        try:
            response = self.session.get(url, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                print(f"请求失败，状态码: {response.status_code}，URL: {url}")
        except Exception as e:
            print(f"请求异常: {e}, URL: {url}")
        return None
    
    def _smart_delay(self):
        """智能延迟，根据并发数动态调整"""
        if self.delay > 0:
            delay = self.delay * (0.8 + random.random() * 0.4)
            time.sleep(delay)
    
    def check_movie_exists(self, dyid):
        """检查影片是否已存在于dy表中"""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT dyid FROM dy WHERE dyid = ?", (dyid,))
                result = cursor.fetchone()
                return result is not None
            finally:
                cursor.close()
    
    def get_existing_m3u8_play_urls(self, dyid):
        """获取指定dyid已存在的play_url列表"""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT play_url FROM m3u8 WHERE dyid = ? AND m3u8_url IS NOT NULL", (dyid,))
                results = cursor.fetchall()
                return [row[0] for row in results] if results else []
            finally:
                cursor.close()
    
    def get_missing_episodes(self, dyid, total_episodes):
        """获取缺失的集数信息"""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("""
                SELECT episode FROM m3u8 
                WHERE dyid = ? AND m3u8_url IS NOT NULL 
                ORDER BY episode
                """, (dyid,))
                existing_episodes = [row[0] for row in cursor.fetchall()]
                all_episodes = set(range(1, total_episodes + 1))
                missing_episodes = list(all_episodes - set(existing_episodes))
                return sorted(missing_episodes)
            finally:
                cursor.close()
    
    def get_total_pages(self, category_id):
        """获取分类的总页数"""
        url = f"{BASE_URL}/list/{category_id}-1.html"
        response = self._get_with_retry(url)
        if not response:
            print(f"获取分类 {category_id} 的总页数失败")
            return 0
            
        soup = BeautifulSoup(response.text, 'lxml')
        
        # 优先查找尾页链接
        last_page_link = soup.select_one('a:-soup-contains("尾页")')
        if last_page_link and 'href' in last_page_link.attrs:
            href = str(last_page_link['href'])
            match = re.search(r'/list/\d+-(\d+)\.html', href)
            if match:
                return int(match.group(1))
        
        # 备用方案：查找所有页码链接
        page_links = soup.select('ul.page a')
        max_page = 0
        for link in page_links:
            text = link.get_text(strip=True)
            if text.isdigit():
                page_num = int(text)
                if page_num > max_page:
                    max_page = page_num
        
        return max_page if max_page > 0 else 1
    
    def get_movie_links_batch(self, category_id, pages):
        """批量获取多页的影片链接"""
        all_links = []
        
        def fetch_page_links(page):
            url = f"{BASE_URL}/list/{category_id}-{page}.html"
            response = self._get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup(response.text, 'lxml')
            movie_links = []
            
            # 查找影片列表
            vodlist_ul = soup.select_one('ul.list_mov') or soup.select_one('ul[class*="-vodlist"]')
            if vodlist_ul:
                vodlist = vodlist_ul.select('li a')
                for link in vodlist:
                    if 'href' in link.attrs:
                        href = str(link['href'])
                        if href.startswith('/mp4/'):
                            full_url = f"{BASE_URL}{href}"
                            movie_links.append(full_url)
            
            return list(dict.fromkeys(movie_links))
        
        # 并发获取多页链接
        with ThreadPoolExecutor(max_workers=min(len(pages), 5)) as executor:
            future_to_page = {executor.submit(fetch_page_links, page): page for page in pages}
            
            for future in as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    links = future.result()
                    all_links.extend(links)
                    print(f"页面 {page}: 获取到 {len(links)} 个链接")
                except Exception as e:
                    print(f"获取页面 {page} 链接失败: {e}")
        
        return list(dict.fromkeys(all_links))
    
    def parse_movie_detail_fast(self, url):
        """快速解析影片详情"""
        response = self._get_with_retry(url, timeout=3)
        if not response:
            return None
        
        # 提取dyid
        match = re.search(r'/mp4/(\d+)\.html', url)
        if not match:
            return None
        dyid = int(match.group(1))
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # 快速提取基本信息
        title_elem = soup.select_one('h1.title')
        name = title_elem.get_text(strip=True).split('(')[0].strip() if title_elem else "未知"
        
        # 批量提取数据元素
        data_elems = soup.select('p.data')
        type_text = region_text = year_text = actors_text = directors_text = "未知"
        
        if data_elems:
            # 类型、地区、年份
            if len(data_elems) > 0:
                type_links = data_elems[0].select('a')
                if type_links:
                    type_text = type_links[0].get_text(strip=True)
                    if len(type_links) > 1:
                        region_text = type_links[1].get_text(strip=True)
                    if len(type_links) > 2:
                        year_text = type_links[2].get_text(strip=True)
            
            # 演员
            if len(data_elems) > 1:
                actor_links = data_elems[1].select('a')
                if actor_links:
                    actors_text = ", ".join([a.get_text(strip=True) for a in actor_links])
            
            # 导演
            if len(data_elems) > 2:
                director_links = data_elems[2].select('a')
                if director_links:
                    directors_text = ", ".join([d.get_text(strip=True) for d in director_links])
        
        # 快速提取简介
        description = "暂无简介"
        desc_elems = soup.select('div[class*="-content__desc"]')
        if len(desc_elems) > 1:
            description = desc_elems[1].get_text(strip=True)
        
        if description == "暂无简介":
            meta_desc = soup.select_one('meta[name="description"]')
            if meta_desc and 'content' in meta_desc.attrs:
                content = str(meta_desc['content'])
                match = re.search(r'剧情[:：](.+)', content)
                if match:
                    description = match.group(1).strip()
                else:
                    description = content.strip()
        
        return {
            'dyid': dyid,
            'name': name,
            'type': type_text,
            'region': region_text,
            'year': year_text,
            'actors': actors_text,
            'directors': directors_text,
            'description': description,
            'url': url
        }
    
    def get_episode_count_fast(self, soup):
        """从已解析的soup中快速获取集数"""
        playlist_ul = soup.select_one('ul[class*="-content__playlist"]')
        if not playlist_ul:
            return 1
        
        playlist_items = playlist_ul.select('li a')
        episode_count = 0
        for item in playlist_items:
            if item.get_text(strip=True) != "APP播放":
                episode_count += 1
        
        return max(episode_count, 1)
    
    def get_m3u8_urls_batch(self, dyid, episode_count, movie_name):
        """批量获取m3u8链接"""
        m3u8_data = []
        
        def fetch_m3u8(episode_index):
            play_url = f"{BASE_URL}/play/{dyid}-0-{episode_index}.html"
            response = self._get_with_retry(play_url, timeout=3)
            if not response:
                return episode_index, play_url, None
            
            # 使用正则表达式快速查找m3u8链接
            content = response.text
            
            # 方法1：查找player_aaaa配置
            player_match = re.search(r'var player_aaaa\s*=\s*({.*?})', content, re.DOTALL)
            if player_match:
                player_data = player_match.group(1)
                url_match = re.search(r"url\s*:\s*'([^']*)'", player_data)
                if url_match:
                    url = url_match.group(1)
                    
                    if "get_dplayer" in url:
                        decrypt_api_url = f"{BASE_URL}{url}"
                        api_response = self._get_with_retry(decrypt_api_url, timeout=3)
                        if api_response:
                            try:
                                api_data = api_response.json()
                                if api_data.get('code') == 200 and api_data.get('url'):
                                    return episode_index, play_url, api_data['url']
                            except:
                                pass
                    elif re.match(r'^[A-Za-z0-9+/=]+$', url) and len(url) > 20:
                        try:
                            decoded_url = base64.b64decode(url).decode('utf-8')
                            if decoded_url.startswith('http'):
                                return episode_index, play_url, decoded_url
                        except:
                            pass
                    elif url.startswith('http'):
                        return episode_index, play_url, url
            
            # 方法2：直接查找m3u8链接
            m3u8_match = re.search(r'(https?://[^\s\'"`,]+\.m3u8)', content)
            if m3u8_match:
                return episode_index, play_url, m3u8_match.group(1)
            
            return episode_index, play_url, None
        
        # 并发获取m3u8链接
        with ThreadPoolExecutor(max_workers=min(episode_count, 5)) as executor:
            future_to_episode = {
                executor.submit(fetch_m3u8, i): i 
                for i in range(episode_count)
            }
            
            for future in as_completed(future_to_episode):
                try:
                    episode_index, play_url, m3u8_url = future.result()
                    m3u8_data.append({
                        'dyid': dyid,
                        'name': movie_name,
                        'episode': episode_index + 1,
                        'play_url': play_url,
                        'm3u8_url': m3u8_url
                    })
                except Exception as e:
                    episode_index = future_to_episode[future]
                    print(f"获取第{episode_index + 1}集m3u8失败: {e}")
        
        return sorted(m3u8_data, key=lambda x: x['episode'])
    
    def get_m3u8_urls_selective(self, dyid, episode_numbers, movie_name):
        """选择性获取指定集数的m3u8链接"""
        m3u8_data = []
        
        def fetch_m3u8(episode_number):
            episode_index = episode_number - 1  # 转换为0基索引
            play_url = f"{BASE_URL}/play/{dyid}-0-{episode_index}.html"
            response = self._get_with_retry(play_url, timeout=3)
            if not response:
                return episode_number, play_url, None
            
            # 使用正则表达式快速查找m3u8链接
            content = response.text
            
            # 方法1：查找player_aaaa配置
            player_match = re.search(r'var player_aaaa\s*=\s*({.*?})', content, re.DOTALL)
            if player_match:
                player_data = player_match.group(1)
                url_match = re.search(r"url\s*:\s*'([^']*)'", player_data)
                if url_match:
                    url = url_match.group(1)
                    
                    if "get_dplayer" in url:
                        decrypt_api_url = f"{BASE_URL}{url}"
                        api_response = self._get_with_retry(decrypt_api_url, timeout=3)
                        if api_response:
                            try:
                                api_data = api_response.json()
                                if api_data.get('code') == 200 and api_data.get('url'):
                                    return episode_number, play_url, api_data['url']
                            except:
                                pass
                    elif re.match(r'^[A-Za-z0-9+/=]+$', url) and len(url) > 20:
                        try:
                            decoded_url = base64.b64decode(url).decode('utf-8')
                            if decoded_url.startswith('http'):
                                return episode_number, play_url, decoded_url
                        except:
                            pass
                    elif url.startswith('http'):
                        return episode_number, play_url, url
            
            # 方法2：直接查找m3u8链接
            m3u8_match = re.search(r'(https?://[^\s\'"`,]+\.m3u8)', content)
            if m3u8_match:
                return episode_number, play_url, m3u8_match.group(1)
            
            return episode_number, play_url, None
        
        # 并发获取指定集数的m3u8链接
        with ThreadPoolExecutor(max_workers=min(len(episode_numbers), 5)) as executor:
            future_to_episode = {
                executor.submit(fetch_m3u8, episode_num): episode_num 
                for episode_num in episode_numbers
            }
            
            for future in as_completed(future_to_episode):
                try:
                    episode_number, play_url, m3u8_url = future.result()
                    m3u8_data.append({
                        'dyid': dyid,
                        'name': movie_name,
                        'episode': episode_number,
                        'play_url': play_url,
                        'm3u8_url': m3u8_url
                    })
                except Exception as e:
                    episode_number = future_to_episode[future]
                    print(f"获取第{episode_number}集m3u8失败: {e}")
        
        return sorted(m3u8_data, key=lambda x: x['episode'])
    
    def batch_save_to_db(self, movies=None, m3u8s=None):
        """批量保存数据到数据库（优化查重）"""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                new_movies = 0
                updated_movies = 0
                new_m3u8s = 0
                updated_m3u8s = 0
                
                if movies:
                    # 批量插入或更新影片信息（只处理新影片）
                    for movie in movies:
                        cursor.execute("SELECT dyid FROM dy WHERE dyid = ?", (movie['dyid'],))
                        if cursor.fetchone():
                            # 更新现有影片信息
                            cursor.execute("""
                            UPDATE dy SET 
                                name = ?, type = ?, region = ?, year = ?, 
                                actors = ?, directors = ?, description = ?, url = ?,
                                crawl_time = CURRENT_TIMESTAMP
                            WHERE dyid = ?
                            """, (
                                movie['name'], movie['type'], movie['region'], movie['year'],
                                movie['actors'], movie['directors'], movie['description'], 
                                movie['url'], movie['dyid']
                            ))
                            updated_movies += 1
                        else:
                            # 插入新影片
                            cursor.execute("""
                            INSERT INTO dy (dyid, name, type, region, year, actors, directors, description, url)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                movie['dyid'], movie['name'], movie['type'], movie['region'], 
                                movie['year'], movie['actors'], movie['directors'], 
                                movie['description'], movie['url']
                            ))
                            new_movies += 1
                
                if m3u8s:
                    # 批量插入或更新m3u8信息（智能查重）
                    for m3u8 in m3u8s:
                        if m3u8['m3u8_url']:  # 只保存有效的m3u8链接
                            cursor.execute("""
                            SELECT id, m3u8_url FROM m3u8 
                            WHERE dyid = ? AND episode = ?
                            """, (m3u8['dyid'], m3u8['episode']))
                            existing = cursor.fetchone()
                            
                            if existing:
                                # 如果已存在但m3u8_url为空，则更新
                                if not existing[1]:  # m3u8_url为空
                                    cursor.execute("""
                                    UPDATE m3u8 SET 
                                        name = ?, play_url = ?, m3u8_url = ?, crawl_time = CURRENT_TIMESTAMP
                                    WHERE dyid = ? AND episode = ?
                                    """, (m3u8['name'], m3u8['play_url'], m3u8['m3u8_url'], 
                                         m3u8['dyid'], m3u8['episode']))
                                    updated_m3u8s += 1
                                # 如果已存在且有m3u8_url，跳过（避免重复）
                            else:
                                # 插入新记录
                                cursor.execute("""
                                INSERT INTO m3u8 (dyid, name, episode, play_url, m3u8_url)
                                VALUES (?, ?, ?, ?, ?)
                                """, (m3u8['dyid'], m3u8['name'], m3u8['episode'], 
                                     m3u8['play_url'], m3u8['m3u8_url']))
                                new_m3u8s += 1
                
                self.conn.commit()
                
                # 输出统计信息
                if new_movies or updated_movies or new_m3u8s or updated_m3u8s:
                    stats = []
                    if new_movies: stats.append(f"新增{new_movies}部影片")
                    if updated_movies: stats.append(f"更新{updated_movies}部影片")
                    if new_m3u8s: stats.append(f"新增{new_m3u8s}个m3u8")
                    if updated_m3u8s: stats.append(f"补充{updated_m3u8s}个m3u8")
                    print(f"💾 批量保存: {', '.join(stats)}")
                
                return True
                
            except Exception as e:
                print(f"批量保存数据失败: {e}")
                self.conn.rollback()
                return False
            finally:
                cursor.close()
    
    def add_to_batch(self, movie_info=None, m3u8_info=None):
        """添加数据到批量处理队列"""
        with self.batch_lock:
            if movie_info:
                self.movie_batch.append(movie_info)
            if m3u8_info:
                self.m3u8_batch.extend(m3u8_info)
            
            # 当批量达到指定大小时，执行保存
            if (len(self.movie_batch) >= self.batch_size or 
                len(self.m3u8_batch) >= self.batch_size * 5):
                self.flush_batch()
    
    def flush_batch(self):
        """刷新批量数据到数据库"""
        if self.movie_batch or self.m3u8_batch:
            movies = self.movie_batch.copy()
            m3u8s = self.m3u8_batch.copy()
            self.movie_batch.clear()
            self.m3u8_batch.clear()
            
            success = self.batch_save_to_db(movies, m3u8s)
            if success:
                print(f"批量保存: {len(movies)}部影片, {len(m3u8s)}个m3u8链接")
            return success
        return True
    
    def crawl_movie_fast(self, url):
        """快速爬取单部影片（带查重功能）"""
        try:
            # 提取dyid进行预检查
            match = re.search(r'/mp4/(\d+)\.html', url)
            if not match:
                print(f"❌ 无法从URL提取dyid: {url}")
                return False
            
            dyid = int(match.group(1))
            
            # 检查影片是否已存在
            movie_exists = self.check_movie_exists(dyid)
            
            # 获取影片详情页面
            response = self._get_with_retry(url, timeout=5)
            if not response:
                return False
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 获取集数
            episode_count = self.get_episode_count_fast(soup)
            
            movie_info = None
            m3u8_data = []
            
            # 获取影片名称（用于m3u8记录）
            movie_name = f"影片{dyid}"  # 默认名称
            
            # 如果影片不存在，需要爬取影片信息
            if not movie_exists:
                movie_info = self.parse_movie_detail_fast(url)
                if not movie_info:
                    return False
                movie_name = movie_info['name']
                print(f"🆕 新影片: {movie_name}")
            else:
                # 影片已存在，从数据库获取名称
                with self.db_lock:
                    cursor = self.conn.cursor()
                    try:
                        cursor.execute("SELECT name FROM dy WHERE dyid = ?", (dyid,))
                        result = cursor.fetchone()
                        if result:
                            movie_name = result[0]
                    finally:
                        cursor.close()
                print(f"📋 已存在影片ID: {dyid} ({movie_name})")
            
            # 检查m3u8链接情况
            missing_episodes = self.get_missing_episodes(dyid, episode_count)
            
            if missing_episodes:
                print(f"🔍 需要补充 {len(missing_episodes)} 集: {missing_episodes}")
                # 只爬取缺失的集数
                m3u8_data = self.get_m3u8_urls_selective(
                    dyid, missing_episodes, movie_name
                )
            else:
                print(f"✅ 所有集数已完整: {episode_count}集")
            
            # 添加到批量处理队列
            if movie_info or m3u8_data:
                self.add_to_batch(movie_info, m3u8_data)
                
                valid_m3u8_count = sum(1 for m in m3u8_data if m['m3u8_url']) if m3u8_data else 0
                status = "新增" if movie_info else "补充"
                print(f"✓ {status} ({episode_count}集, {valid_m3u8_count}个新链接)")
            
            return True
            
        except Exception as e:
            print(f"爬取影片失败 {url}: {e}")
            return False
    
    def save_progress(self, category, current_page, total_pages, last_dyid, status="running"):
        """保存爬取进度"""
        with self.db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT id FROM crawl_progress WHERE category = ?", (category,))
                if cursor.fetchone():
                    cursor.execute("""
                    UPDATE crawl_progress SET 
                        current_page = ?, total_pages = ?, last_dyid = ?, 
                        status = ?, update_time = CURRENT_TIMESTAMP
                    WHERE category = ?
                    """, (current_page, total_pages, last_dyid, status, category))
                else:
                    cursor.execute("""
                    INSERT INTO crawl_progress (category, current_page, total_pages, last_dyid, status)
                    VALUES (?, ?, ?, ?, ?)
                    """, (category, current_page, total_pages, last_dyid, status))
                
                self.conn.commit()
                return True
            except Exception as e:
                print(f"保存爬取进度失败: {e}")
                self.conn.rollback()
                return False
            finally:
                cursor.close()
    
    def get_progress(self, category):
        """获取指定分类的爬取进度"""
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT category, current_page, total_pages, last_dyid, status
            FROM crawl_progress WHERE category = ?
            """, (category,))
            
            row = cursor.fetchone()
            cursor.close()
            if row:
                return dict(row)
            return None
    
    def crawl_category_optimized(self, category_id, start_page=None):
        """优化的分类爬取方法"""
        category_name = CATEGORIES.get(category_id, f"分类{category_id}")
        print(f"🚀 开始高速爬取{category_name}...")
        
        current_page = 1
        total_pages = 0
        
        try:
            # 获取或恢复进度
            progress = self.get_progress(category_id)
            if progress and progress['status'] == 'running' and start_page is None:
                current_page = progress['current_page']
                total_pages = progress['total_pages']
                print(f"📋 恢复爬取进度: 当前页 {current_page}/{total_pages}")
            else:
                total_pages = self.get_total_pages(category_id)
                if total_pages == 0:
                    print(f"❌ 获取{category_name}总页数失败")
                    return False
                current_page = 1 if start_page is None else start_page
                print(f"📊 {category_name}总页数: {total_pages}")
            
            if self.test_mode and total_pages > 2:
                total_pages = 2
                print("🧪 测试模式: 只爬取前2页")
            
            self.save_progress(category_id, current_page, total_pages, 0, "running")
            
            # 按批次处理页面
            batch_size = min(3, total_pages - current_page + 1)  # 每批处理3页
            
            for batch_start in range(current_page, total_pages + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, total_pages)
                batch_pages = list(range(batch_start, batch_end + 1))
                
                print(f"📦 批量处理页面 {batch_start}-{batch_end}")
                
                # 批量获取影片链接
                movie_links = self.get_movie_links_batch(category_id, batch_pages)
                print(f"🔗 获取到 {len(movie_links)} 个影片链接")
                
                if not movie_links:
                    continue
                
                # 并发爬取影片
                success_count = 0
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 提交所有任务
                    future_to_url = {
                        executor.submit(self.crawl_movie_fast, url): url 
                        for url in movie_links
                    }
                    
                    # 使用进度条显示处理进度
                    with tqdm(total=len(movie_links), desc=f"爬取进度") as pbar:
                        for future in as_completed(future_to_url):
                            url = future_to_url[future]
                            try:
                                if future.result():
                                    success_count += 1
                            except Exception as e:
                                print(f"❌ 处理失败 {url}: {e}")
                            finally:
                                pbar.update(1)
                                self._smart_delay()
                
                # 刷新批量数据
                self.flush_batch()
                
                # 更新进度
                self.save_progress(category_id, batch_end, total_pages, 0, "running")
                
                print(f"✅ 批次完成: {success_count}/{len(movie_links)} 成功")
            
            # 最终刷新
            self.flush_batch()
            self.save_progress(category_id, total_pages, total_pages, 0, "completed")
            print(f"🎉 {category_name}爬取完成!")
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ 爬取被用户中断")
            self.flush_batch()
            self.save_progress(category_id, current_page, total_pages, 0, "interrupted")
            return False
        except Exception as e:
            print(f"💥 爬取过程中发生错误: {e}")
            self.flush_batch()
            self.save_progress(category_id, current_page, total_pages, 0, "error")
            return False
    
    def crawl_all_optimized(self):
        """优化的全分类爬取"""
        print("🚀 开始高速爬取所有分类...")
        for category_id in CATEGORIES:
            if not self.crawl_category_optimized(category_id):
                print("⚠️ 爬取被中断或出错，停止所有爬取任务。")
                break
            print(f"⏱️ 等待 {self.delay * 2} 秒后继续下一个分类...")
            time.sleep(self.delay * 2)
    
    def close(self):
        """关闭资源"""
        self.flush_batch()  # 确保所有数据都已保存
        if self.session:
            self.session.close()
        if self.conn:
            self.conn.close()

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="DSQ4D影视资源高速爬虫")
    parser.add_argument("--test", action="store_true", help="测试模式，每个分类只爬取前2页")
    parser.add_argument("--category", type=int, choices=list(CATEGORIES.keys()), help="指定要爬取的分类ID (1=电影, 2=电视剧, 3=动漫, 4=综艺, 19=大陆剧, 20=欧美剧, 21=香港剧, 22=韩国剧, 23=台湾剧, 24=日本剧, 25=海外剧, 26=泰国剧, 27=短剧)")
    parser.add_argument("--page", type=int, help="指定从哪一页开始爬取")
    parser.add_argument("--delay", type=float, default=0.1, help="请求延迟时间(秒)")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数")
    parser.add_argument("--batch-size", type=int, default=50, help="批量处理大小")
    
    args = parser.parse_args()
    
    print("🎬 DSQ4D高速爬虫启动中...")
    print(f"⚙️ 配置: 并发数={args.workers}, 延迟={args.delay}s, 批量大小={args.batch_size}")
    
    crawler = OptimizedDSQ4DCrawler(
        test_mode=args.test,
        delay=args.delay,
        max_workers=args.workers,
        batch_size=args.batch_size
    )
    
    try:
        if args.category:
            crawler.crawl_category_optimized(args.category, args.page)
        else:
            crawler.crawl_all_optimized()
    finally:
        crawler.close()
        print("🏁 爬虫已关闭")

if __name__ == "__main__":
    main()
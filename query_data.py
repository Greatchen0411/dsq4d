#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import argparse
import csv
import json
import os
import sys
from datetime import datetime

# 数据库文件
DB_FILE = "dy.db"

def connect_db():
    """连接数据库"""
    if not os.path.exists(DB_FILE):
        print(f"数据库文件 {DB_FILE} 不存在，请先运行爬虫程序")
        sys.exit(1)
    
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_categories():
    """获取所有分类"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT DISTINCT type FROM dy
    """)
    
    categories = [row['type'] for row in cursor.fetchall()]
    conn.close()
    
    return categories

def get_progress():
    """获取爬取进度"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT category, current_page, total_pages, status, update_time
    FROM crawl_progress
    """)
    
    progress = cursor.fetchall()
    conn.close()
    
    return progress

def get_movie_count():
    """获取影片数量"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM dy")
    movie_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM m3u8")
    m3u8_count = cursor.fetchone()['count']
    
    conn.close()
    
    return movie_count, m3u8_count

def search_movies(keyword=None, category=None, region=None, year=None, limit=100):
    """搜索影片"""
    conn = connect_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM dy WHERE 1=1"
    params = []
    
    if keyword:
        query += " AND (name LIKE ? OR description LIKE ?)"
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    
    if category:
        query += " AND type = ?"
        params.append(category)
    
    if region:
        query += " AND region = ?"
        params.append(region)
    
    if year:
        query += " AND year = ?"
        params.append(year)
    
    query += " ORDER BY dyid DESC LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    movies = cursor.fetchall()
    conn.close()
    
    return movies

def get_m3u8_links(dyid):
    """获取指定影片的m3u8链接"""
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT m.*, d.name as movie_name
    FROM m3u8 m
    JOIN dy d ON m.dyid = d.dyid
    WHERE m.dyid = ?
    ORDER BY m.episode
    """, (dyid,))
    
    links = cursor.fetchall()
    conn.close()
    
    return links

def export_to_csv(data, filename):
    """导出数据到CSV文件"""
    if not data:
        print("没有数据可导出")
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # 写入表头
            writer.writerow(dict(data[0]).keys())
            
            # 写入数据
            for row in data:
                writer.writerow(dict(row).values())
        
        print(f"数据已导出到 {filename}")
        return True
    except Exception as e:
        print(f"导出数据失败: {e}")
        return False

def export_to_json(data, filename):
    """导出数据到JSON文件"""
    if not data:
        print("没有数据可导出")
        return False
    
    try:
        # 将sqlite3.Row对象转换为字典
        json_data = [dict(row) for row in data]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"数据已导出到 {filename}")
        return True
    except Exception as e:
        print(f"导出数据失败: {e}")
        return False

def export_m3u8_playlist(links, filename):
    """导出m3u8链接为播放列表"""
    if not links:
        print("没有m3u8链接可导出")
        return False
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            
            for link in links:
                f.write(f"#EXTINF:-1,{link['movie_name']} 第{link['episode']}集\n")
                f.write(f"{link['m3u8_url']}\n")
        
        print(f"播放列表已导出到 {filename}")
        return True
    except Exception as e:
        print(f"导出播放列表失败: {e}")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="DSQ4D影视资源数据查询工具")
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # 查看进度
    progress_parser = subparsers.add_parser("progress", help="查看爬取进度")
    
    # 查看统计信息
    stats_parser = subparsers.add_parser("stats", help="查看统计信息")
    
    # 搜索影片
    search_parser = subparsers.add_parser("search", help="搜索影片")
    search_parser.add_argument("-k", "--keyword", help="关键词")
    search_parser.add_argument("-c", "--category", help="分类")
    search_parser.add_argument("-r", "--region", help="地区")
    search_parser.add_argument("-y", "--year", help="年份")
    search_parser.add_argument("-l", "--limit", type=int, default=100, help="限制结果数量")
    search_parser.add_argument("-o", "--output", help="导出文件名")
    search_parser.add_argument("-f", "--format", choices=["csv", "json"], default="csv", help="导出格式")
    
    # 获取m3u8链接
    m3u8_parser = subparsers.add_parser("m3u8", help="获取m3u8链接")
    m3u8_parser.add_argument("dyid", type=int, help="影片ID")
    m3u8_parser.add_argument("-o", "--output", help="导出文件名")
    m3u8_parser.add_argument("-f", "--format", choices=["csv", "json", "m3u"], default="m3u", help="导出格式")
    
    args = parser.parse_args()
    
    if args.command == "progress":
        # 查看爬取进度
        progress = get_progress()
        if progress:
            print("爬取进度:")
            for row in progress:
                category_id = row['category']
                category_name = {1: "电影", 2: "电视剧", 3: "动漫", 4: "综艺", 27: "短剧"}.get(category_id, f"分类{category_id}")
                print(f"{category_name}: {row['current_page']}/{row['total_pages']} 页, 状态: {row['status']}, 更新时间: {row['update_time']}")
        else:
            print("没有爬取进度记录")
    
    elif args.command == "stats":
        # 查看统计信息
        movie_count, m3u8_count = get_movie_count()
        categories = get_categories()
        
        print("统计信息:")
        print(f"影片总数: {movie_count}")
        print(f"m3u8链接总数: {m3u8_count}")
        print(f"影片分类: {', '.join(categories)}")
    
    elif args.command == "search":
        # 搜索影片
        movies = search_movies(args.keyword, args.category, args.region, args.year, args.limit)
        
        if movies:
            print(f"找到 {len(movies)} 部影片:")
            for i, movie in enumerate(movies[:10]):  # 只显示前10条
                print(f"{i+1}. {movie['name']} ({movie['year']}) - {movie['type']} - {movie['region']}")
            
            if len(movies) > 10:
                print(f"... 还有 {len(movies) - 10} 部影片未显示")
            
            # 导出数据
            if args.output:
                if args.format == "csv":
                    export_to_csv(movies, args.output)
                elif args.format == "json":
                    export_to_json(movies, args.output)
        else:
            print("没有找到符合条件的影片")
    
    elif args.command == "m3u8":
        # 获取m3u8链接
        links = get_m3u8_links(args.dyid)
        
        if links:
            print(f"找到 {len(links)} 个m3u8链接:")
            for link in links:
                print(f"第{link['episode']}集: {link['m3u8_url']}")
            
            # 导出数据
            if args.output:
                if args.format == "csv":
                    export_to_csv(links, args.output)
                elif args.format == "json":
                    export_to_json(links, args.output)
                elif args.format == "m3u":
                    export_m3u8_playlist(links, args.output)
        else:
            print(f"没有找到影片ID为 {args.dyid} 的m3u8链接")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys

def init_database():
    """初始化数据库，创建必要的表"""
    # 检查数据库文件是否存在
    db_file = 'dy.db'
    db_exists = os.path.exists(db_file)
    
    # 连接数据库
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # 创建影片信息表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dy (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dyid INTEGER UNIQUE,
        name TEXT,
        type TEXT,
        region TEXT,
        year TEXT,
        actors TEXT,
        directors TEXT,
        description TEXT,
        url TEXT,
        crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建m3u8链接表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS m3u8 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dyid INTEGER,
        name TEXT,
        episode INTEGER,
        play_url TEXT,
        m3u8_url TEXT,
        crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (dyid) REFERENCES dy (dyid)
    )
    ''')
    
    # 创建爬取进度表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS crawl_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category INTEGER,
        current_page INTEGER,
        total_pages INTEGER,
        last_dyid INTEGER,
        status TEXT,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 提交更改
    conn.commit()
    conn.close()
    
    print(f"数据库{'已存在' if db_exists else '已创建'}")
    print("所有表已创建/更新")

if __name__ == "__main__":
    init_database()
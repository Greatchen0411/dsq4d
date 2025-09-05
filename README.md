# DSQ4D 影视资源爬虫

这是一个用于爬取 https://m.dsq4d.com/ 网站影视资源的爬虫程序。

## 功能特点

- 支持爬取电影、电视剧、动漫、综艺、短剧五大分类
- 提取影片基本信息（名称、类型、地区、年份、主演、导演、简介）
- 提取影片的m3u8播放链接
- 支持断点续传，可以暂停后继续爬取
- 支持测试模式，每个分类只爬取前2页
- 数据存储到SQLite数据库中，方便查询和使用

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. 初始化数据库

首次使用前，需要初始化数据库：

```bash
python init_db.py
```

或者使用启动器的初始化选项：

```bash
python start_crawler.py --init
```

### 2. 启动爬虫

#### 爬取所有分类

```bash
python start_crawler.py
```

#### 测试模式（每个分类只爬取前2页）

```bash
python start_crawler.py --test
```

#### 爬取指定分类

```bash
# 爬取电影分类
python start_crawler.py --category 1

# 爬取电视剧分类
python start_crawler.py --category 2

# 爬取动漫分类
python start_crawler.py --category 3

# 爬取综艺分类
python start_crawler.py --category 4

# 爬取短剧分类
python start_crawler.py --category 27
```

#### 从指定页码开始爬取

```bash
python start_crawler.py --category 1 --page 10
```

#### 设置请求延迟时间

```bash
python start_crawler.py --delay 2.0
```

### 3. 查询数据

#### 查看爬取进度

```bash
python query_data.py progress
```

#### 查看统计信息

```bash
python query_data.py stats
```

#### 搜索影片

```bash
# 基本搜索
python query_data.py search --keyword "龙"

# 按分类搜索
python query_data.py search --category "动作片"

# 按地区搜索
python query_data.py search --region "中国大陆"

# 按年份搜索
python query_data.py search --year "2025"

# 组合搜索
python query_data.py search --keyword "龙" --category "动作片" --year "2025"

# 限制结果数量
python query_data.py search --keyword "龙" --limit 50

# 导出搜索结果到CSV文件
python query_data.py search --keyword "龙" --output "results.csv"

# 导出搜索结果到JSON文件
python query_data.py search --keyword "龙" --output "results.json" --format json
```

#### 获取m3u8链接

```bash
# 获取指定影片的m3u8链接
python query_data.py m3u8 199745

# 导出m3u8链接到播放列表
python query_data.py m3u8 199745 --output "playlist.m3u"

# 导出m3u8链接到CSV文件
python query_data.py m3u8 199745 --output "links.csv" --format csv

# 导出m3u8链接到JSON文件
python query_data.py m3u8 199745 --output "links.json" --format json
```

## 数据库结构

### dy表（影片信息）

- id: 自增主键
- dyid: 影片唯一ID
- name: 影片名称
- type: 影片类型
- region: 地区
- year: 年份
- actors: 主演
- directors: 导演
- description: 影片简介
- url: 影片详情页URL
- crawl_time: 爬取时间

### m3u8表（播放链接）

- id: 自增主键
- dyid: 影片ID（关联dy表）
- name: 影片名称
- episode: 集数
- play_url: 播放页面URL
- m3u8_url: m3u8链接
- crawl_time: 爬取时间

### crawl_progress表（爬取进度）

- id: 自增主键
- category: 分类ID
- current_page: 当前页码
- total_pages: 总页数
- last_dyid: 最后爬取的影片ID
- status: 状态（running/completed/interrupted/error）
- update_time: 更新时间

## 注意事项

- 爬取过程中可以按Ctrl+C中断，下次启动时会自动从中断处继续爬取
- 测试模式下每个分类只爬取前2页，适合用于测试程序是否正常工作
- 请合理设置请求延迟时间，避免对目标网站造成过大压力
- 本程序仅供学习和研究使用，请勿用于商业用途
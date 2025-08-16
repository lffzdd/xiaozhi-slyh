# rag_updater.py (这是一个独立的脚本，可以放在任何地方)
import asyncio
import redis.asyncio as redis
import json
import logging
import asyncpg
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

db_pool = None  # 全局数据库连接池

async def init_postgresql_pool():
    global db_pool
    # 数据库连接池
    db_pool = await asyncpg.create_pool(
        user='root',
        password='123456',
        database='chat_db',
        host='localhost',  # 或者你的数据库服务器地址
        port=5432  # PostgreSQL 默认端口
    )
    # 创建表（如果不存在）
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS interactions (
                id SERIAL PRIMARY KEY,
                type TEXT NOT NULL,
                session_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                text TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("💿 表 interactions 创建成功.")

# --- 在这里定义你的数据库写入逻辑 ---
async def write_to_rag_db(data):
    """
    这是一个示例函数，你需要替换成你自己的数据库写入实现。
    比如使用 aiosqlite, aiomysql, asyncpg 等。
    """
    async with db_pool.acquire() as conn:
        timestamp = data.get('timestamp')
        if isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp)
        await conn.execute(
            "INSERT INTO interactions (type, session_id, device_id, text, timestamp) VALUES ($1, $2, $3, $4, $5)",
            data.get('type'), data.get('session_id'), data.get('device_id'), data.get('text'), timestamp
        )
    await asyncio.sleep(0.1) # 模拟数据库IO
    logging.info("写入完成.")
# --- 数据库逻辑结束 ---


async def redis_subscriber():
    """连接到 Redis 并订阅频道"""
    r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
    async with r.pubsub() as pubsub:
        # 订阅我们之前定义的频道
        await pubsub.subscribe('xiaozhi_interactions')
        logging.info("已订阅频道 'xiaozhi_interactions'，等待消息...")
        
        while True:
            try:
                # 等待消息
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if message and message['type'] == 'message':
                    # 解析 JSON 数据
                    data = json.loads(message['data'])
                    # 打印接收到的消息
                    logging.info(f"接收到消息: {data}")
                    # 异步处理数据写入
                    asyncio.create_task(write_to_rag_db(data))

            except Exception as e:
                logging.error(f"处理消息时出错: {e}")
                await asyncio.sleep(5) # 出错时等待一段时间再重试

async def main():
    # 初始化 PostgreSQL 连接池
    await init_postgresql_pool()
    try:
        await redis_subscriber()
    except KeyboardInterrupt:
        logging.info("程序已停止.")

if __name__ == "__main__":
    asyncio.run(main())
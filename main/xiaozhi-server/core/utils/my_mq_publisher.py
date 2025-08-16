import redis.asyncio as redis
import json
import asyncio
from config.logger import setup_logging

# 定义一个全局的 Redis 连接池
redis_pool = None
LOGGER = setup_logging()
TAG = __name__

def init_redis_pool(host='localhost', port=6379, db=1):
    """
    初始化 Redis 连接池
    """
    global redis_pool
    if redis_pool is None:
        LOGGER.bind(tag=TAG).info(f"初始化 Redis 连接池 -> {host}:{port}")
        redis_pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)

async def publish_message(channel, message_data):
    """
    异步发布消息到指定的 Redis Channel
    :param channel: 频道名称 (e.g., 'xiaozhi_interactions')
    :param message_data: 要发送的字典数据
    """
    if redis_pool is None:
        LOGGER.bind(tag=TAG).warning("Redis 连接池未初始化，无法发布消息")
        return

    try:
        # 使用异步上下文管理器自动处理连接的关闭
        async with redis.Redis(connection_pool=redis_pool) as r:
            # 将字典转换为 JSON 字符串
            message_json = json.dumps(message_data, ensure_ascii=False)
            # 发布消息
            await r.publish(channel, message_json)
            LOGGER.bind(tag=TAG).debug(f"成功发布消息到频道 '{channel}': {message_json}")
    except Exception as e:
        LOGGER.bind(tag=TAG).error(f"发布 Redis 消息失败: {e}")
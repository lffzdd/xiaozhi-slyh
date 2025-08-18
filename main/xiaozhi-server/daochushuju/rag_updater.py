# rag_updater_fixed.py
import asyncio
import redis.asyncio as redis
import json
import logging
import asyncpg
from datetime import datetime
import os
from zhipuai import ZhipuAI

# --- 配置 (与您原版相同) ---
# ZHIPUAI_API_KEY = os.getenv("ZHIPUAI_API_KEY", "ca13e6189e2d4fb7b81946c5d77b9219.fNMK7R94xrqvdxzo")
ZHIPUAI_API_KEY = "ca13e6189e2d4fb7b81946c5d77b9219.fNMK7R94xrqvdxzo"

DB_USER = 'root'
DB_PASS = '123456'
DB_NAME = 'chat_db'
DB_HOST = 'localhost'
DB_PORT = 5432
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 1
# --- 配置结束 ---

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# --- 全局变量 ---
db_pool = None
llm_client = None
# ✨ 新增：用于跟踪所有正在运行的后台任务
active_tasks = set()
# ✨ 新增：用于优雅关闭的事件
shutdown_event = asyncio.Event()


async def init_services():
    """初始化数据库连接池和LLM客户端 (与您原版基本相同)"""
    global db_pool, llm_client
    logging.info("🚀 开始初始化服务...")
    try:
        logging.info("📊 正在连接PostgreSQL数据库...")
        db_pool = await asyncio.wait_for(
            asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT, min_size=1, max_size=10),
            timeout=10.0
        )
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS interactions (
                    id SERIAL PRIMARY KEY, type TEXT NOT NULL, session_id TEXT NOT NULL,
                    device_id TEXT NOT NULL, text TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    speaker TEXT
                )''')
            logging.info("💿 表 'interactions' 已准备就绪.")
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_profiles (
                    id SERIAL PRIMARY KEY, device_id TEXT NOT NULL UNIQUE,
                    profile_data JSONB, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
            logging.info("💿 表 'user_profiles' 已准备就绪.")
        logging.info("✅ PostgreSQL数据库连接成功！")
    except Exception as e:
        logging.error(f"❌ 初始化数据库失败: {e}")
        raise

    logging.info("🤖 正在初始化LLM客户端...")
    if not ZHIPUAI_API_KEY:
        logging.warning("⚠️ 未配置有效的 ZHIPUAI_API_KEY，用户画像总结功能将无法使用。")
    else:
        llm_client = ZhipuAI(api_key=ZHIPUAI_API_KEY)
        logging.info("🤖 LLM 客户端初始化成功。")
    logging.info("✅ 所有服务初始化完成！")

# --- LLM 总结部分 (与您原版相同) ---
async def summarize_conversation_with_llm(conversation_text, existing_profile):
    """使用大语言模型总结对话并更新用户画像"""
    if not llm_client:
        logging.warning("LLM 客户端未初始化，跳过总结。")
        return existing_profile or {}
    prompt = f"""
你是一个资深的用户行为分析师。请根据以下用户与AI的对话记录，总结并更新该用户的个人画像。
画像应以JSON格式输出，包含但不限于以下字段：
- interests (兴趣爱好), personality (性格特点), known_facts (已知信息), potential_needs (潜在需求)
请在已有画像的基础上进行补充和更新，而不是完全替换。
[已有画像]
{json.dumps(existing_profile, indent=2, ensure_ascii=False)}
[本次对话记录]
{conversation_text}
[你的输出 (请只返回JSON对象)]
"""
    try:
        logging.info(f"正在为以下对话调用 LLM：\n---\n{conversation_text[:200]}...\n---")
        response = await asyncio.to_thread(
            llm_client.chat.completions.create,
            model="glm-4-flash-250414",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        summary_json_str = response.choices[0].message.content
        new_profile = json.loads(summary_json_str)
        logging.info(f"✅ LLM 总结完成，生成新画像: {new_profile}")
        return new_profile
    except Exception as e:
        logging.error(f"❌ 调用 LLM 失败: {e}", exc_info=True) # exc_info=True 会打印完整的错误堆栈
        return existing_profile


async def process_session_summary(session_id, device_id):
    """处理单个会话的总结任务"""
    # 💡 改进：去掉了不可靠的 sleep(0.5)。
    # 假设 session_end 消息总是在对话消息之后到达。
    logging.info(f"开始处理会话总结: session_id={session_id}, device_id={device_id}")
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch("SELECT type, text, speaker FROM interactions WHERE session_id = $1 ORDER BY timestamp ASC", session_id)
            if not records:
                logging.warning(f"未找到会话 {session_id} 的任何记录，无法总结。")
                return

            conversation_text = "\n".join(f"{rec['speaker'] or rec['type']}: {rec['text']}" for rec in records)
            profile_record = await conn.fetchrow("SELECT profile_data FROM user_profiles WHERE device_id = $1", device_id)
            existing_profile = profile_record['profile_data'] if profile_record else {}
            
            logging.info(f"获取到设备 {device_id} 的现有画像: {existing_profile}")
            new_profile = await summarize_conversation_with_llm(conversation_text, existing_profile)

            # 只有在画像确实更新了的情况下才写入数据库
            if new_profile and new_profile != existing_profile:
                await conn.execute(
                    """
                    INSERT INTO user_profiles (device_id, profile_data, last_updated) VALUES ($1, $2, NOW())
                    ON CONFLICT (device_id) DO UPDATE SET profile_data = $2, last_updated = NOW();
                    """,
                    device_id, json.dumps(new_profile)
                )
                logging.info(f"✅ 成功更新设备 {device_id} 的用户画像。")
            else:
                logging.info(f"画像无变化，未更新数据库。")

    except Exception as e:
        # 确保即使在这个任务内部出错，我们也能看到日志
        logging.error(f"❌ 处理会话总结 {session_id} 时发生严重错误: {e}", exc_info=True)


async def save_interaction(data):
    """将单条交互消息存入数据库"""
    try:
        async with db_pool.acquire() as conn:
            timestamp = data.get('timestamp')
            if isinstance(timestamp, (int, float)):
                timestamp = datetime.fromtimestamp(timestamp)
            
            await conn.execute(
                """
                INSERT INTO interactions (type, session_id, device_id, text, speaker, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                data.get('type'), data.get('session_id'), data.get('device_id'),
                data.get('text'), data.get('speaker'), timestamp
            )
            logging.info(f"💾 已保存消息到数据库: session={data.get('session_id')}, type={data.get('type')}")
    except Exception as e:
        logging.error(f"❌ 保存交互信息到数据库失败: {e}", exc_info=True)


def create_safe_task(coro):
    """
    ✨ 新增：一个安全的创建任务的函数。
    它会创建任务，并将其添加到我们的全局任务集合中，
    并设置一个“完成回调”来处理异常和清理。
    """
    task = asyncio.create_task(coro)
    active_tasks.add(task)
    # 当任务完成时（无论成功、失败还是取消），这个回调函数都会被调用
    task.add_done_callback(task_done_callback)
    return task

def task_done_callback(task):
    """✨ 新增：任务完成时的回调函数"""
    try:
        # 如果任务在执行过程中出现了异常，调用 .result() 会重新抛出该异常
        task.result()
    except asyncio.CancelledError:
        pass  # 任务被取消是正常情况，不用记录错误
    except Exception as e:
        # 💥 这里是关键！捕获并记录所有“被遗忘的”任务中的异常
        logging.error(f"后台任务发生未捕获的异常: {e}", exc_info=True)
    finally:
        # 无论如何，都将任务从活动集合中移除
        active_tasks.remove(task)


async def message_consumer(queue):
    """
    ✨ 新增：消息消费者。
    它从队列中获取消息并进行处理，将逻辑解耦。
    """
    while not (shutdown_event.is_set() and queue.empty()):
        try:
            message_data = await asyncio.wait_for(queue.get(), timeout=1.0)
            data = json.loads(message_data)
            
            if data.get('type') == 'session_end':
                logging.info(f"检测到会话结束信号: {data.get('session_id')}，准备触发总结。")
                create_safe_task(process_session_summary(data.get('session_id'), data.get('device_id')))
            else:
                await save_interaction(data)
            queue.task_done()
        except asyncio.TimeoutError:
            continue # 队列为空，继续等待
        except json.JSONDecodeError:
            logging.warning(f"无法解析消息: {message_data}")
            queue.task_done()
        except Exception as e:
            logging.error(f"处理消息时出错: {e}", exc_info=True)
            queue.task_done()


async def redis_producer(queue):
    """
    ✨ 重构：现在只负责从Redis接收消息并放入内部队列。
    """
    logging.info("🔄 开始连接Redis...")
    while not shutdown_event.is_set():
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            async with r.pubsub() as pubsub:
                await pubsub.subscribe('xiaozhi_interactions')
                logging.info(f"已订阅频道 'xiaozhi_interactions'，在 {REDIS_HOST}:{REDIS_PORT} 等待消息...")
                while not shutdown_event.is_set():
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message and message['type'] == 'message':
                        logging.info(f"📥 从Redis接收到消息，放入处理队列...")
                        await queue.put(message['data'])
        except redis.exceptions.ConnectionError as e:
            if not shutdown_event.is_set():
                logging.error(f"Redis 连接失败: {e}. 5秒后重试...")
                await asyncio.sleep(5)
        except Exception as e:
            if not shutdown_event.is_set():
                logging.error(f"Redis生产者发生未知错误: {e}. 5秒后重试...", exc_info=True)
                await asyncio.sleep(5)



async def main():
    """主程序入口 (Windows 兼容版)"""
    
    # 初始化服务
    try:
        await init_services()
    except Exception as e:
        logging.critical(f"服务初始化失败，程序无法启动: {e}")
        return # 如果服务都起不来，直接退出

    # 核心改动：使用队列来解耦生产者和消费者
    message_queue = asyncio.Queue(maxsize=100)
    
    producer_task = asyncio.create_task(redis_producer(message_queue))
    consumer_task = asyncio.create_task(message_consumer(message_queue))

    logging.info("🚀 服务已启动，按 CTRL+C 退出。")

    # ✨ 核心修改：使用 try...except KeyboardInterrupt 来处理 Windows 上的 Ctrl+C
    try:
        # 在 Linux/macOS 上，仍然可以使用信号
        # 但为了跨平台一致性，这里我们统一使用更简单的方式
        # 持续运行，直到 producer 或 consumer 意外退出
        await asyncio.gather(producer_task, consumer_task)

    except KeyboardInterrupt:
        logging.info("收到 CTRL+C 信号！正在准备优雅退出...")
    except asyncio.CancelledError:
        # 如果任务被外部取消，也视为正常退出
        logging.info("主任务被取消，开始关闭流程...")
    
    finally:
        # 触发关闭事件，通知所有循环任务停止
        shutdown_event.set()

        logging.info("正在关闭程序...")
        
        # 1. 给生产者和消费者一点时间来响应 shutdown_event
        # gather 它们，让它们有机会干净地退出循环
        await asyncio.gather(producer_task, consumer_task, return_exceptions=True)
        logging.info("生产者和消费者任务已停止。")
        
        # 2. 等待队列中的现有消息被处理完毕
        # 因为消费者可能已经停止，我们需要一个新的小任务来清空队列
        final_consumer = asyncio.create_task(message_consumer(message_queue))
        await message_queue.join()
        final_consumer.cancel()
        await asyncio.gather(final_consumer, return_exceptions=True)
        logging.info("消息队列已清空。")

        # 3. 等待所有后台任务（如LLM调用）完成
        if active_tasks:
            logging.info(f"等待 {len(active_tasks)} 个后台任务完成...")
            # 设置一个超时，防止任务永远卡住
            done, pending = await asyncio.wait(active_tasks, timeout=30.0)
            
            if pending:
                logging.warning(f"{len(pending)} 个后台任务超时，将被强制取消。")
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

            logging.info("所有后台任务已处理完毕。")

        # 4. 关闭数据库连接池
        if db_pool:
            await db_pool.close()
            logging.info("数据库连接池已关闭。")
            
        logging.info("👋 程序已优雅退出。")


if __name__ == "__main__":
    # ✨ 修改：这里也用 try/except 包裹一下，可以捕获启动过程中的 Ctrl+C
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("程序在启动时被强制退出。")
import asyncio
import random
import logging
import traceback
import sys
import os
import json
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import FloodWaitError, BadRequestError, RPCError, NetworkMigrateError, PhoneMigrateError, TimedOutError, AuthKeyError
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.updates import GetStateRequest
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from collections import deque

from dotenv import load_dotenv
load_dotenv()

if getattr(sys, 'frozen', False):
    SCRIPT_DIR = Path(sys.executable).parent
else:
    SCRIPT_DIR = Path(__file__).parent.absolute()

DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION_FILE = DATA_DIR / "nft_gift_monitor.session"
LOG_FILE = DATA_DIR / "monitor.log"
STATS_FILE = DATA_DIR / "statistics.json"
HISTORY_FILE = DATA_DIR / "listings_history.json"

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = str(SESSION_FILE.with_suffix(''))
GROUP_ID = int(os.getenv('GROUP_ID'))
GROUP_INVITE = os.getenv('GROUP_INVITE')

TARGET_GIFT_NAMES = [
    "Heart Locket", "Durov's Cap", "Precious Peach", "Heroic Helmet",
    "Perfume Bottle", "Magic Potion", "Nail Bracelet", "Mini Oscar",
    "Mighty Arm", "Ion Gem", "Gem Signet", "Artisan Brick",
    "Genie Lamp", "Electric Skull", "Sharp Tongue", "Bling Binky",
    "Bonded Ring", "Kissed Frog", "Loot Bag", "Scared Cat",
    "Westside Sign", "Neko Helmet", "Low Rider", "Signet Ring",
    "Astral Shard", "Swiss Watch", "Voodoo Doll"
]

BASE_SCAN_INTERVAL = (15, 30)
CONCURRENT_REQUESTS = 3
FETCH_LIMIT = 30
CONCURRENT_ALERTS = 5
LISTING_MEMORY_HOURS = 48
OWNER_CACHE_TTL_HOURS = 6
OWNER_CACHE_MAX_SIZE = 2000
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60
KEEPALIVE_INTERVAL = 240
MIN_REQUEST_DELAY = 3.5
MAX_REQUEST_DELAY = 7.0
BATCH_DELAY_MIN = 6.0
BATCH_DELAY_MAX = 12.0
CIRCUIT_BREAKER_THRESHOLD = 3
CIRCUIT_BREAKER_TIMEOUT = 300
HEALTH_CHECK_INTERVAL = 15
SAVE_STATS_INTERVAL = 300

logger = logging.getLogger('NFTMonitor')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)

seen_listings: Set[str] = set()
listing_timestamps: Dict[str, datetime] = {}
owner_cache: Dict[int, Tuple[Optional[str], datetime]] = {}
last_request_times = deque(maxlen=50)
error_history = deque(maxlen=100)
circuit_breaker_until: Optional[datetime] = None
consecutive_errors = 0
health_status = {"connected": True, "last_success": datetime.now(), "error_rate": 0.0}
start_time = datetime.now()

stats = {
    'scans': 0,
    'alerts': 0,
    'errors': 0,
    'skipped_no_owner': 0,
    'reconnects': 0,
    'flood_waits': 0,
    'circuit_breaks': 0,
    'successful_requests': 0,
    'failed_requests': 0,
    'start_time': start_time.isoformat(),
    'total_listings_found': 0,
    'unique_gifts_seen': set(),
    'hourly_alerts': {}
}

listings_history = []

def load_stats():
    global stats, start_time
    try:
        if STATS_FILE.exists():
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                for key in ['scans', 'alerts', 'errors', 'skipped_no_owner', 'reconnects', 'flood_waits', 'circuit_breaks', 'successful_requests', 'failed_requests', 'total_listings_found']:
                    if key in loaded:
                        stats[key] = loaded[key]
                stats['unique_gifts_seen'] = set(loaded.get('unique_gifts_seen', []))
                stats['hourly_alerts'] = loaded.get('hourly_alerts', {})
                if 'start_time' in loaded:
                    start_time = datetime.fromisoformat(loaded['start_time'])
                    stats['start_time'] = loaded['start_time']
                logger.info("✓ Статистика загружена")
    except Exception as e:
        logger.warning(f"Не удалось загрузить статистику: {e}")

def save_stats():
    try:
        stats_copy = stats.copy()
        stats_copy['unique_gifts_seen'] = list(stats['unique_gifts_seen'])
        stats_copy['uptime_seconds'] = (datetime.now() - start_time).total_seconds()
        stats_copy['last_updated'] = datetime.now().isoformat()
        stats_copy['error_rate'] = get_error_rate()
        stats_copy['seen_listings_count'] = len(seen_listings)
        stats_copy['owner_cache_size'] = len(owner_cache)
        
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stats_copy, f, ensure_ascii=False, indent=2)
        
        logger.debug("✓ Статистика сохранена")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения статистики: {e}")

def load_history():
    global listings_history
    try:
        if HISTORY_FILE.exists():
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                listings_history = json.load(f)
            logger.info(f"✓ История загружена: {len(listings_history)} записей")
    except Exception as e:
        logger.warning(f"Не удалось загрузить историю: {e}")

def get_error_rate() -> float:
    if not error_history:
        return 0.0
    recent_window = datetime.now() - timedelta(minutes=15)
    recent_errors = sum(1 for ts, is_error in error_history if ts > recent_window and is_error)
    total_recent = sum(1 for ts, _ in error_history if ts > recent_window)
    return recent_errors / max(total_recent, 1)

def update_health():
    global health_status
    health_status["error_rate"] = get_error_rate()
    if stats['successful_requests'] > 0:
        health_status["last_success"] = datetime.now()

def get_adaptive_delay() -> Tuple[int, int]:
    error_rate = get_error_rate()
    base_min, base_max = BASE_SCAN_INTERVAL
    
    if error_rate > 0.3:
        multiplier = 2.5
    elif error_rate > 0.15:
        multiplier = 1.8
    elif error_rate > 0.05:
        multiplier = 1.3
    else:
        multiplier = 1.0
    
    return (int(base_min * multiplier), int(base_max * multiplier))

def cleanup_old():
    cutoff = datetime.now() - timedelta(hours=LISTING_MEMORY_HOURS)
    cache_cutoff = datetime.now() - timedelta(hours=OWNER_CACHE_TTL_HOURS)
    global seen_listings, listing_timestamps, owner_cache
    
    old_listings = {k for k, v in listing_timestamps.items() if v <= cutoff}
    seen_listings -= old_listings
    listing_timestamps = {k: v for k, v in listing_timestamps.items() if v > cutoff}
    
    owner_cache = {k: v for k, v in owner_cache.items() if v[1] > cache_cutoff}
    
    if len(owner_cache) > OWNER_CACHE_MAX_SIZE:
        sorted_cache = sorted(owner_cache.items(), key=lambda x: x[1][1], reverse=True)
        owner_cache = dict(sorted_cache[:int(OWNER_CACHE_MAX_SIZE * 0.8)])

def check_circuit_breaker() -> bool:
    global circuit_breaker_until, consecutive_errors
    
    if circuit_breaker_until and datetime.now() < circuit_breaker_until:
        return False
    
    if circuit_breaker_until and datetime.now() >= circuit_breaker_until:
        logger.info("✓ Автостоп сброшен")
        circuit_breaker_until = None
        consecutive_errors = 0
    
    return True

def trigger_circuit_breaker():
    global circuit_breaker_until, consecutive_errors
    consecutive_errors += 1
    
    if consecutive_errors >= CIRCUIT_BREAKER_THRESHOLD:
        circuit_breaker_until = datetime.now() + timedelta(seconds=CIRCUIT_BREAKER_TIMEOUT)
        stats['circuit_breaks'] += 1
        logger.warning(f"⚠ Автостоп! Пауза {CIRCUIT_BREAKER_TIMEOUT}с")
        consecutive_errors = 0

def reset_circuit_breaker():
    global consecutive_errors
    consecutive_errors = 0

async def adaptive_rate_limit():
    now = datetime.now()
    last_request_times.append(now)
    
    if len(last_request_times) >= 2:
        time_since_last = (now - last_request_times[-2]).total_seconds()
        min_delay = MIN_REQUEST_DELAY + (get_error_rate() * 4)
        
        if time_since_last < min_delay:
            jitter = random.uniform(0.5, 1.5)
            await asyncio.sleep(min_delay - time_since_last + jitter)

async def safe_request(client: TelegramClient, request_func, *args, max_retries=MAX_RETRIES, critical=False, **kwargs):
    if not check_circuit_breaker():
        wait_time = (circuit_breaker_until - datetime.now()).total_seconds()
        if wait_time > 0:
            await asyncio.sleep(min(wait_time + 1, 15))
    
    for attempt in range(max_retries):
        try:
            await adaptive_rate_limit()
            result = await asyncio.wait_for(request_func(*args, **kwargs), timeout=REQUEST_TIMEOUT)
            
            stats['successful_requests'] += 1
            error_history.append((datetime.now(), False))
            reset_circuit_breaker()
            update_health()
            return result
            
        except FloodWaitError as e:
            stats['flood_waits'] += 1
            stats['failed_requests'] += 1
            error_history.append((datetime.now(), True))
            
            wait_time = min(e.seconds + random.randint(20, 60), 1200)
            logger.warning(f"⏱ FloodWait {wait_time}с")
            
            trigger_circuit_breaker()
            await asyncio.sleep(wait_time)
            
        except (TimedOutError, asyncio.TimeoutError):
            stats['failed_requests'] += 1
            error_history.append((datetime.now(), True))
            logger.warning(f"⏱ Таймаут (попытка {attempt+1}/{max_retries})")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(random.uniform(8, 20))
                continue
            
            if critical:
                trigger_circuit_breaker()
            raise
            
        except (NetworkMigrateError, PhoneMigrateError) as e:
            logger.warning(f"🔄 Миграция: {e}")
            await client.disconnect()
            await asyncio.sleep(random.uniform(5, 10))
            await client.connect()
            await asyncio.sleep(3)
            continue
            
        except AuthKeyError as e:
            logger.error(f"❌ Ошибка авторизации: {e}")
            stats['errors'] += 1
            raise
            
        except (BadRequestError, RPCError) as e:
            stats['failed_requests'] += 1
            error_history.append((datetime.now(), True))
            error_str = str(e).upper()
            
            if "FLOOD" in error_str or "SLOWMODE" in error_str or "WAIT" in error_str:
                wait_time = random.randint(180, 420)
                logger.warning(f"⏱ Flood/Slowmode {wait_time}с")
                trigger_circuit_breaker()
                await asyncio.sleep(wait_time)
                continue
            
            logger.error(f"❌ RPC ошибка: {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(random.uniform(12, 25))
                continue
            
            if critical:
                trigger_circuit_breaker()
            raise
            
        except Exception as e:
            stats['failed_requests'] += 1
            error_history.append((datetime.now(), True))
            logger.error(f"❌ Неожиданная ошибка: {type(e).__name__}: {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(random.uniform(8, 20))
                continue
            
            if critical:
                trigger_circuit_breaker()
            raise
    
    return None

async def ensure_connected(client: TelegramClient) -> bool:
    try:
        if not client.is_connected():
            logger.warning("🔌 Переподключение...")
            stats['reconnects'] += 1
            await client.connect()
            await asyncio.sleep(random.uniform(3, 6))
            await client.get_me()
            logger.info("✓ Переподключено")
            return True
        return True
    except Exception as e:
        logger.error(f"❌ Переподключение не удалось: {e}")
        health_status["connected"] = False
        return False

async def stats_saver_task():
    while True:
        try:
            await asyncio.sleep(SAVE_STATS_INTERVAL)
            save_stats()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения статистики: {e}")

async def keepalive_task(client: TelegramClient):
    while True:
        try:
            jitter = random.randint(-30, 30)
            await asyncio.sleep(KEEPALIVE_INTERVAL + jitter)
            
            if not check_circuit_breaker():
                continue
            
            connected = await ensure_connected(client)
            if connected:
                await safe_request(client, client, GetStateRequest(), max_retries=2, critical=False)
                health_status["connected"] = True
                logger.debug("✓ Keepalive OK")
            
        except Exception as e:
            logger.error(f"❌ Ошибка keepalive: {e}")
            health_status["connected"] = False
            await asyncio.sleep(60)

async def health_monitor_task():
    while True:
        try:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            
            error_rate = get_error_rate()
            time_since_success = (datetime.now() - health_status["last_success"]).total_seconds()
            
            if error_rate > 0.5:
                logger.warning(f"⚠ Высокий процент ошибок: {error_rate:.1%}")
            
            if time_since_success > 600:
                logger.warning(f"⚠ Нет успешных запросов {time_since_success:.0f}с")
            
        except Exception as e:
            logger.error(f"Ошибка мониторинга: {e}")

async def get_available_gifts(client: TelegramClient) -> List[dict]:
    try:
        if not await ensure_connected(client):
            return []
        
        result = await safe_request(client, client, GetStarGiftsRequest(hash=0), critical=True)
        if not result or not hasattr(result, 'gifts'):
            return []

        gifts = [
            {'id': gift.id, 'title': gift.title}
            for gift in result.gifts
            if hasattr(gift, 'title') and gift.title in TARGET_GIFT_NAMES
        ]

        logger.info(f"✓ Найдено {len(gifts)} целевых подарков")
        return gifts

    except Exception as e:
        stats['errors'] += 1
        logger.error(f"❌ Ошибка получения подарков: {e}")
        return []

async def check_owner(client: TelegramClient, owner_id) -> Optional[str]:
    if not owner_id:
        return None
    
    try:
        user_id = owner_id.user_id if hasattr(owner_id, 'user_id') else owner_id
        
        if user_id in owner_cache:
            username, cached_time = owner_cache[user_id]
            cache_age = (datetime.now() - cached_time).total_seconds() / 3600
            if cache_age < OWNER_CACHE_TTL_HOURS:
                return username
        
        if not await ensure_connected(client):
            return None
        
        await asyncio.sleep(random.uniform(0.3, 0.8))
        
        user = await safe_request(client, client.get_entity, owner_id, max_retries=2, critical=False)
        if not user:
            owner_cache[user_id] = (None, datetime.now())
            return None
        
        username = None
        if hasattr(user, 'username') and user.username:
            username = f"@{user.username}"
        elif hasattr(user, 'first_name') and user.first_name:
            username = user.first_name
        
        owner_cache[user_id] = (username, datetime.now())
        return username
        
    except:
        return None

async def fetch_fresh_listings(
    client: TelegramClient,
    gift_id: int,
    gift_name: str,
    semaphore: asyncio.Semaphore
) -> List[dict]:
    async with semaphore:
        try:
            if not check_circuit_breaker():
                return []
            
            if not await ensure_connected(client):
                return []
            
            delay = random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY)
            if get_error_rate() > 0.15:
                delay *= 1.8
            await asyncio.sleep(delay)
            
            result = await safe_request(
                client,
                client,
                GetResaleStarGiftsRequest(
                    gift_id=gift_id,
                    offset="",
                    limit=FETCH_LIMIT,
                    sort_by_num=False,
                    sort_by_price=False
                ),
                critical=True
            )

            if not result or not hasattr(result, 'gifts'):
                return []

            listings = [
                {
                    'title': gift_name,
                    'slug': gift.slug,
                    'number': gift.num,
                    'owner_id': getattr(gift, 'owner_id', None),
                    'listing_id': f"{gift.slug}-{gift.num}",
                }
                for gift in result.gifts
                if hasattr(gift, 'num') and hasattr(gift, 'slug')
            ]
            
            stats['total_listings_found'] += len(listings)
            stats['unique_gifts_seen'].add(gift_name)
            
            return listings

        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + random.randint(20, 60))
            return []
        except Exception as e:
            stats['errors'] += 1
            logger.debug(f"Ошибка загрузки {gift_name}: {e}")
            return []

async def scan_all_gifts(client: TelegramClient, gifts: List[dict]) -> List[dict]:
    if not gifts:
        return []
    
    shuffled = gifts.copy()
    random.shuffle(shuffled)
    
    logger.info(f"🔍 Сканируем {len(shuffled)} подарков")
    
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    all_listings = []
    
    batch_size = 1 if get_error_rate() > 0.1 else 2
    
    for i in range(0, len(shuffled), batch_size):
        if not check_circuit_breaker():
            logger.warning("⏸ Автостоп, пропуск батча")
            await asyncio.sleep(15)
            continue
        
        batch = shuffled[i:i+batch_size]
        tasks = [fetch_fresh_listings(client, g['id'], g['title'], semaphore) for g in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, list):
                all_listings.extend(result)
        
        if i + batch_size < len(shuffled):
            delay = random.uniform(BATCH_DELAY_MIN, BATCH_DELAY_MAX)
            if get_error_rate() > 0.15:
                delay *= 1.5
            await asyncio.sleep(delay)
    
    return all_listings

async def send_all_alerts_optimized(
    client: TelegramClient,
    chat_entity,
    listings: List[dict]
) -> None:
    if not listings:
        return
    
    logger.info(f"📤 Отправка {len(listings)} алертов")
    semaphore = asyncio.Semaphore(CONCURRENT_ALERTS)
    
    async def send_one(listing):
        async with semaphore:
            try:
                if not check_circuit_breaker() or not await ensure_connected(client):
                    return
                
                owner = await check_owner(client, listing.get('owner_id'))
                
                if not owner:
                    stats['skipped_no_owner'] += 1
                    return
                
                link = f"https://t.me/nft/{listing['slug']}-{listing['number']}"
                msg = f"**{listing['title']}** `#{listing['number']}`\n👤 {owner}\n{link}"
                
                await asyncio.sleep(random.uniform(1.5, 3.5))
                
                await safe_request(
                    client,
                    client.send_message,
                    chat_entity,
                    msg,
                    link_preview=True,
                    parse_mode='Markdown',
                    critical=False
                )
                stats['alerts'] += 1
                
                history_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'title': listing['title'],
                    'slug': listing['slug'],
                    'number': listing['number'],
                    'listing_id': listing['listing_id'],
                    'owner': owner,
                    'link': link
                }
                listings_history.append(history_entry)
                
                current_hour = datetime.now().strftime('%Y-%m-%d %H:00')
                stats['hourly_alerts'][current_hour] = stats['hourly_alerts'].get(current_hour, 0) + 1
                
            except Exception as e:
                logger.error(f"❌ Ошибка отправки: {e}")
    
    await asyncio.gather(*[send_one(l) for l in listings], return_exceptions=True)
    logger.info(f"✓ Отправлено: {stats['alerts']} | Пропущено: {stats['skipped_no_owner']}")

async def main():
    logger.info("=" * 60)
    logger.info("NFT MONITOR by wortexhf [SECURE MODE]")
    logger.info("=" * 60)
    logger.info(f"📁 Данные: {DATA_DIR}")
    logger.info("")
    
    load_stats()
    load_history()
    
    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        connection_retries=5,
        retry_delay=8,
        auto_reconnect=True,
        timeout=60,
        request_retries=3,
        flood_sleep_threshold=180
    )
    
    chat_entity = None
    keepalive = None
    health_monitor = None
    stats_saver = None
    
    try:
        await client.start()
        await asyncio.sleep(random.uniform(3, 6))
        me = await client.get_me()
        logger.info(f"✓ Подключено: {me.first_name}")
        health_status["connected"] = True
        
        keepalive = asyncio.create_task(keepalive_task(client))
        health_monitor = asyncio.create_task(health_monitor_task())
        stats_saver = asyncio.create_task(stats_saver_task())
        
        gifts = await get_available_gifts(client)
        if not gifts:
            logger.error("❌ Нет целевых подарков")
            return
        
        await asyncio.sleep(random.uniform(3, 6))
        
        try:
            chat_entity = await safe_request(client, client.get_entity, GROUP_INVITE, critical=True)
        except:
            chat_entity = await safe_request(client, client.get_entity, GROUP_ID, critical=True)
        
        logger.info(f"✓ Группа подключена")
        await asyncio.sleep(random.uniform(4, 8))
        
        logger.info("🔍 Начальное сканирование...")
        initial = await scan_all_gifts(client, gifts)
        
        now = datetime.now()
        for listing in initial:
            listing_id = listing['listing_id']
            seen_listings.add(listing_id)
            listing_timestamps[listing_id] = now
        
        logger.info(f"✓ База: {len(seen_listings)} листингов")
        logger.info(f"✓ Мониторинг запущен")
        logger.info(f"⚙ Безопасный режим:")
        logger.info(f"  - Все {len(gifts)} подарков каждый скан")
        logger.info(f"  - Интервал: {BASE_SCAN_INTERVAL[0]}-{BASE_SCAN_INTERVAL[1]}с")
        logger.info(f"  - Проверка владельцев: включена")
        logger.info(f"  - Автостоп: {CIRCUIT_BREAKER_THRESHOLD} ошибок")
        logger.info("")
        
        while True:
            try:
                if not await ensure_connected(client):
                    await asyncio.sleep(60)
                    continue
                
                stats['scans'] += 1
                scan_start = datetime.now()
                
                logger.info(f"{'='*60}")
                logger.info(f"СКАН #{stats['scans']}")
                logger.info(f"{'='*60}")
                
                all_listings = await scan_all_gifts(client, gifts)
                logger.info(f"✓ Найдено {len(all_listings)} листингов")
                
                now = datetime.now()
                current_listing_ids = {l['listing_id'] for l in all_listings}
                new_listing_ids = current_listing_ids - seen_listings
                
                if new_listing_ids:
                    new_listings = [l for l in all_listings if l['listing_id'] in new_listing_ids]
                    logger.info(f"🆕 Новых: {len(new_listings)}")
                    
                    for listing_id in new_listing_ids:
                        seen_listings.add(listing_id)
                        listing_timestamps[listing_id] = now
                    
                    await send_all_alerts_optimized(client, chat_entity, new_listings)
                else:
                    logger.info("⏸ Новых нет")
                
                cleanup_old()
                update_health()
                
                error_rate = get_error_rate()
                uptime = (datetime.now() - start_time).total_seconds()
                
                logger.info(f"📊 Статистика:")
                logger.info(
                    f"  Алертов: {stats['alerts']} | "
                    f"Пропущено (no owner): {stats['skipped_no_owner']} | "
                    f"Кеш: {len(owner_cache)}"
                )
                logger.info(
                    f"  Успешно: {stats['successful_requests']} | "
                    f"Ошибок: {stats['failed_requests']} | "
                    f"Error rate: {error_rate:.1%}"
                )
                logger.info(
                    f"  FloodWait: {stats['flood_waits']} | "
                    f"Автостопов: {stats['circuit_breaks']} | "
                    f"Uptime: {uptime/3600:.1f}ч"
                )
                
                scan_duration = (datetime.now() - scan_start).total_seconds()
                delay_range = get_adaptive_delay()
                delay = random.randint(*delay_range)
                
                if error_rate > 0.2:
                    logger.info(f"⚠ Увеличенная пауза")
                
                logger.info(f"⏸ Пауза {delay}с (скан {scan_duration:.1f}с)")
                logger.info("")
                
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"❌ Ошибка цикла: {e}")
                stats['errors'] += 1
                error_history.append((datetime.now(), True))
                
                delay = random.randint(60, 120)
                logger.info(f"⏸ Восстановление {delay}с")
                await asyncio.sleep(delay)
                
                try:
                    await ensure_connected(client)
                except:
                    pass
    
    except KeyboardInterrupt:
        logger.info("\n⏹ Остановлено")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
    finally:
        if keepalive:
            keepalive.cancel()
        if health_monitor:
            health_monitor.cancel()
        if stats_saver:
            stats_saver.cancel()
        
        save_stats()
        
        try:
            with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(listings_history[-1000:], f, ensure_ascii=False, indent=2)
        except:
            pass
        
        try:
            await client.disconnect()
        except:
            pass
        
        logger.info("✓ Отключено")

if __name__ == "__main__":
    try:
        while True:
            try:
                asyncio.run(main())
                logger.info("✓ Завершено")
                break
            except KeyboardInterrupt:
                logger.info("⏹ Остановлено")
                sys.exit(0)
            except EOFError:
                logger.error("❌ Нет сессии")
                sys.exit(1)
            except Exception as e:
                delay = random.randint(60, 120)
                logger.error(f"❌ Перезапуск через {delay}с")
                traceback.print_exc()
                asyncio.run(asyncio.sleep(delay))
    except KeyboardInterrupt:
        sys.exit(0)
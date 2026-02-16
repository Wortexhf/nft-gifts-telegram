import asyncio
import random
import traceback
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from collections import deque

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, BadRequestError, RPCError, NetworkMigrateError, 
    PhoneMigrateError, TimedOutError, AuthKeyError
)
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest
from telethon.tl.functions.updates import GetStateRequest

import config
from utils import logger

class NFTMonitor:
    def __init__(self):
        self.seen_listings: Set[str] = set()
        self.listing_timestamps: Dict[str, datetime] = {}
        self.owner_cache: Dict[int, Tuple[Optional[str], datetime]] = {}
        self.last_request_times = deque(maxlen=50)
        self.error_history = deque(maxlen=100)
        self.circuit_breaker_until: Optional[datetime] = None
        self.consecutive_errors = 0
        self.health_status = {"connected": True, "last_success": datetime.now(), "error_rate": 0.0}
        self.start_time = datetime.now()
        
        self.stats = {
            'scans': 0, 'alerts': 0, 'errors': 0, 'skipped_no_owner': 0,
            'reconnects': 0, 'flood_waits': 0, 'circuit_breaks': 0,
            'successful_requests': 0, 'failed_requests': 0,
            'start_time': self.start_time.isoformat(),
            'total_listings_found': 0,
            'unique_gifts_seen': set(),
            'hourly_alerts': {}
        }
        self.listings_history = []
        
        self.client = TelegramClient(
            config.SESSION_NAME,
            config.API_ID,
            config.API_HASH,
            connection_retries=5,
            retry_delay=8,
            auto_reconnect=True,
            timeout=60,
            request_retries=3,
            flood_sleep_threshold=180
        )

    def load_stats(self):
        try:
            if config.STATS_FILE.exists():
                with open(config.STATS_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    for key in ['scans', 'alerts', 'errors', 'skipped_no_owner', 'reconnects', 'flood_waits', 'circuit_breaks', 'successful_requests', 'failed_requests', 'total_listings_found']:
                        if key in loaded:
                            self.stats[key] = loaded[key]
                    self.stats['unique_gifts_seen'] = set(loaded.get('unique_gifts_seen', []))
                    self.stats['hourly_alerts'] = loaded.get('hourly_alerts', {})
                    if 'start_time' in loaded:
                        self.start_time = datetime.fromisoformat(loaded['start_time'])
                        self.stats['start_time'] = loaded['start_time']
                    logger.info("✓ Статистика загружена")
        except Exception as e:
            logger.warning(f"Не удалось загрузить статистику: {e}")

    def save_stats(self):
        try:
            stats_copy = self.stats.copy()
            stats_copy['unique_gifts_seen'] = list(self.stats['unique_gifts_seen'])
            stats_copy['uptime_seconds'] = (datetime.now() - self.start_time).total_seconds()
            stats_copy['last_updated'] = datetime.now().isoformat()
            stats_copy['error_rate'] = self.get_error_rate()
            stats_copy['seen_listings_count'] = len(self.seen_listings)
            stats_copy['owner_cache_size'] = len(self.owner_cache)
            
            with open(config.STATS_FILE, 'w', encoding='utf-8') as f:
                json.dump(stats_copy, f, ensure_ascii=False, indent=2)
            
            logger.debug("✓ Статистика сохранена")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения статистики: {e}")

    def load_history(self):
        try:
            if config.HISTORY_FILE.exists():
                with open(config.HISTORY_FILE, 'r', encoding='utf-8') as f:
                    self.listings_history = json.load(f)
                logger.info(f"✓ История загружена: {len(self.listings_history)} записей")
        except Exception as e:
            logger.warning(f"Не удалось загрузить историю: {e}")

    def get_error_rate(self) -> float:
        if not self.error_history:
            return 0.0
        recent_window = datetime.now() - timedelta(minutes=15)
        recent_errors = sum(1 for ts, is_error in self.error_history if ts > recent_window and is_error)
        total_recent = sum(1 for ts, _ in self.error_history if ts > recent_window)
        return recent_errors / max(total_recent, 1)

    def update_health(self):
        self.health_status["error_rate"] = self.get_error_rate()
        if self.stats['successful_requests'] > 0:
            self.health_status["last_success"] = datetime.now()

    def get_adaptive_delay(self) -> Tuple[int, int]:
        error_rate = self.get_error_rate()
        base_min, base_max = config.BASE_SCAN_INTERVAL
        
        if error_rate > 0.3:
            multiplier = 2.5
        elif error_rate > 0.15:
            multiplier = 1.8
        elif error_rate > 0.05:
            multiplier = 1.3
        else:
            multiplier = 1.0
        
        return (int(base_min * multiplier), int(base_max * multiplier))

    def cleanup_old(self):
        cutoff = datetime.now() - timedelta(hours=config.LISTING_MEMORY_HOURS)
        cache_cutoff = datetime.now() - timedelta(hours=config.OWNER_CACHE_TTL_HOURS)
        
        old_listings = {k for k, v in self.listing_timestamps.items() if v <= cutoff}
        self.seen_listings -= old_listings
        self.listing_timestamps = {k: v for k, v in self.listing_timestamps.items() if v > cutoff}
        
        self.owner_cache = {k: v for k, v in self.owner_cache.items() if v[1] > cache_cutoff}
        
        if len(self.owner_cache) > config.OWNER_CACHE_MAX_SIZE:
            sorted_cache = sorted(self.owner_cache.items(), key=lambda x: x[1][1], reverse=True)
            self.owner_cache = dict(sorted_cache[:int(config.OWNER_CACHE_MAX_SIZE * 0.8)])

    def check_circuit_breaker(self) -> bool:
        if self.circuit_breaker_until and datetime.now() < self.circuit_breaker_until:
            return False
        
        if self.circuit_breaker_until and datetime.now() >= self.circuit_breaker_until:
            logger.info("✓ Автостоп сброшен")
            self.circuit_breaker_until = None
            self.consecutive_errors = 0
        
        return True

    def trigger_circuit_breaker(self):
        self.consecutive_errors += 1
        
        if self.consecutive_errors >= config.CIRCUIT_BREAKER_THRESHOLD:
            self.circuit_breaker_until = datetime.now() + timedelta(seconds=config.CIRCUIT_BREAKER_TIMEOUT)
            self.stats['circuit_breaks'] += 1
            logger.warning(f"⚠ Автостоп! Пауза {config.CIRCUIT_BREAKER_TIMEOUT}с")
            self.consecutive_errors = 0

    def reset_circuit_breaker(self):
        self.consecutive_errors = 0

    async def adaptive_rate_limit(self):
        now = datetime.now()
        self.last_request_times.append(now)
        
        if len(self.last_request_times) >= 2:
            time_since_last = (now - self.last_request_times[-2]).total_seconds()
            min_delay = config.MIN_REQUEST_DELAY + (self.get_error_rate() * 4)
            
            if time_since_last < min_delay:
                jitter = random.uniform(0.5, 1.5)
                await asyncio.sleep(min_delay - time_since_last + jitter)

    async def safe_request(self, client: TelegramClient, request_func, *args, max_retries=config.MAX_RETRIES, critical=False, **kwargs):
        if not self.check_circuit_breaker():
            wait_time = (self.circuit_breaker_until - datetime.now()).total_seconds()
            if wait_time > 0:
                await asyncio.sleep(min(wait_time + 1, 15))
        
        for attempt in range(max_retries):
            try:
                await self.adaptive_rate_limit()
                result = await asyncio.wait_for(request_func(*args, **kwargs), timeout=config.REQUEST_TIMEOUT)
                
                self.stats['successful_requests'] += 1
                self.error_history.append((datetime.now(), False))
                self.reset_circuit_breaker()
                self.update_health()
                return result
                
            except FloodWaitError as e:
                self.stats['flood_waits'] += 1
                self.stats['failed_requests'] += 1
                self.error_history.append((datetime.now(), True))
                
                wait_time = min(e.seconds + random.randint(20, 60), 1200)
                logger.warning(f"⏱ FloodWait {wait_time}с")
                
                self.trigger_circuit_breaker()
                await asyncio.sleep(wait_time)
                
            except (TimedOutError, asyncio.TimeoutError):
                self.stats['failed_requests'] += 1
                self.error_history.append((datetime.now(), True))
                logger.warning(f"⏱ Таймаут (попытка {attempt+1}/{max_retries})")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(8, 20))
                    continue
                
                if critical:
                    self.trigger_circuit_breaker()
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
                self.stats['errors'] += 1
                raise
                
            except (BadRequestError, RPCError) as e:
                self.stats['failed_requests'] += 1
                self.error_history.append((datetime.now(), True))
                error_str = str(e).upper()
                
                if "FLOOD" in error_str or "SLOWMODE" in error_str or "WAIT" in error_str:
                    wait_time = random.randint(180, 420)
                    logger.warning(f"⏱ Flood/Slowmode {wait_time}с")
                    self.trigger_circuit_breaker()
                    await asyncio.sleep(wait_time)
                    continue
                
                logger.error(f"❌ RPC ошибка: {e}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(12, 25))
                    continue
                
                if critical:
                    self.trigger_circuit_breaker()
                raise
                
            except Exception as e:
                self.stats['failed_requests'] += 1
                self.error_history.append((datetime.now(), True))
                logger.error(f"❌ Неожиданная ошибка: {type(e).__name__}: {e}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(8, 20))
                    continue
                
                if critical:
                    self.trigger_circuit_breaker()
                raise
        
        return None

    async def ensure_connected(self, client: TelegramClient) -> bool:
        try:
            if not client.is_connected():
                logger.warning("🔌 Переподключение...")
                self.stats['reconnects'] += 1
                await client.connect()
                await asyncio.sleep(random.uniform(3, 6))
                await client.get_me()
                logger.info("✓ Переподключено")
                return True
            return True
        except Exception as e:
            logger.error(f"❌ Переподключение не удалось: {e}")
            self.health_status["connected"] = False
            return False

    async def stats_saver_task(self):
        while True:
            try:
                await asyncio.sleep(config.SAVE_STATS_INTERVAL)
                self.save_stats()
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения статистики: {e}")

    async def keepalive_task(self, client: TelegramClient):
        while True:
            try:
                jitter = random.randint(-30, 30)
                await asyncio.sleep(config.KEEPALIVE_INTERVAL + jitter)
                
                if not self.check_circuit_breaker():
                    continue
                
                connected = await self.ensure_connected(client)
                if connected:
                    await self.safe_request(client, client, GetStateRequest(), max_retries=2, critical=False)
                    self.health_status["connected"] = True
                    logger.debug("✓ Keepalive OK")
                
            except Exception as e:
                logger.error(f"❌ Ошибка keepalive: {e}")
                self.health_status["connected"] = False
                await asyncio.sleep(60)

    async def health_monitor_task(self):
        while True:
            try:
                await asyncio.sleep(config.HEALTH_CHECK_INTERVAL)
                
                error_rate = self.get_error_rate()
                time_since_success = (datetime.now() - self.health_status["last_success"]).total_seconds()
                
                if error_rate > 0.5:
                    logger.warning(f"⚠ Высокий процент ошибок: {error_rate:.1%}")
                
                if time_since_success > 600:
                    logger.warning(f"⚠ Нет успешных запросов {time_since_success:.0f}с")
                
            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")

    async def get_available_gifts(self, client: TelegramClient) -> List[dict]:
        try:
            if not await self.ensure_connected(client):
                return []
            
            result = await self.safe_request(client, client, GetStarGiftsRequest(hash=0), critical=True)
            if not result or not hasattr(result, 'gifts'):
                return []

            gifts = [
                {'id': gift.id, 'title': gift.title}
                for gift in result.gifts
                if hasattr(gift, 'title') and gift.title in config.TARGET_GIFT_NAMES
            ]

            logger.info(f"✓ Найдено {len(gifts)} целевых подарков")
            return gifts

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Ошибка получения подарков: {e}")
            return []

    async def check_owner(self, client: TelegramClient, owner_id) -> Optional[str]:
        if not owner_id:
            return None
        
        try:
            user_id = owner_id.user_id if hasattr(owner_id, 'user_id') else owner_id
            
            if user_id in self.owner_cache:
                username, cached_time = self.owner_cache[user_id]
                cache_age = (datetime.now() - cached_time).total_seconds() / 3600
                if cache_age < config.OWNER_CACHE_TTL_HOURS:
                    return username
            
            if not await self.ensure_connected(client):
                return None
            
            await asyncio.sleep(random.uniform(0.3, 0.8))
            
            user = await self.safe_request(client, client.get_entity, owner_id, max_retries=2, critical=False)
            if not user:
                self.owner_cache[user_id] = (None, datetime.now())
                return None
            
            username = None
            if hasattr(user, 'username') and user.username:
                username = f"@{user.username}"
            else:
                first_name = getattr(user, 'first_name', 'Unknown') or 'Unknown'
                # Escape markdown characters in name just in case
                first_name = first_name.replace('[', '').replace(']', '')
                username = f"[{first_name}](tg://user?id={user.id})"
            
            self.owner_cache[user_id] = (username, datetime.now())
            return username
            
        except:
            return None

    async def fetch_fresh_listings(
        self,
        client: TelegramClient,
        gift_id: int,
        gift_name: str,
        semaphore: asyncio.Semaphore
    ) -> List[dict]:
        async with semaphore:
            try:
                if not self.check_circuit_breaker():
                    return []
                
                if not await self.ensure_connected(client):
                    return []
                
                delay = random.uniform(config.MIN_REQUEST_DELAY, config.MAX_REQUEST_DELAY)
                if self.get_error_rate() > 0.15:
                    delay *= 1.8
                await asyncio.sleep(delay)
                
                result = await self.safe_request(
                    client,
                    client,
                    GetResaleStarGiftsRequest(
                        gift_id=gift_id,
                        offset="",
                        limit=config.FETCH_LIMIT,
                        sort_by_num=False,
                        sort_by_price=False
                    ),
                    critical=True
                )

                if not result or not hasattr(result, 'gifts'):
                    return []

                listings = []
                for gift in result.gifts:
                    if hasattr(gift, 'num') and hasattr(gift, 'slug'):
                        # Try to get price
                        price = getattr(gift, 'price', None)
                        
                        # Include price in ID to detect price changes if needed
                        price_suffix = f"-{price}" if price is not None else ""
                        
                        listings.append({
                            'title': gift_name,
                            'slug': gift.slug,
                            'number': gift.num,
                            'price': price,
                            'owner_id': getattr(gift, 'owner_id', None),
                            'listing_id': f"{gift.slug}-{gift.num}",
                        })
                
                self.stats['total_listings_found'] += len(listings)
                self.stats['unique_gifts_seen'].add(gift_name)
                
                return listings

            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + random.randint(20, 60))
                return []
            except Exception as e:
                self.stats['errors'] += 1
                logger.debug(f"Ошибка загрузки {gift_name}: {e}")
                return []

    async def scan_all_gifts(self, client: TelegramClient, gifts: List[dict]) -> List[dict]:
        if not gifts:
            return []
        
        shuffled = gifts.copy()
        random.shuffle(shuffled)
        
        total_gifts = len(shuffled)
        logger.info(f"🔍 Сканируем {total_gifts} подарков")
        
        semaphore = asyncio.Semaphore(config.CONCURRENT_REQUESTS)
        all_listings = []
        
        batch_size = 1 if self.get_error_rate() > 0.1 else 3
        
        for i in range(0, total_gifts, batch_size):
            if not self.check_circuit_breaker():
                logger.warning("⏸ Автостоп, пропуск батча")
                await asyncio.sleep(15)
                continue
            
            batch = shuffled[i:i+batch_size]
            
            # Log progress
            logger.info(f"⏳ Батч {i//batch_size + 1}/{(total_gifts + batch_size - 1)//batch_size} ({len(batch)} шт: {', '.join(g['title'] for g in batch)})")

            tasks = [self.fetch_fresh_listings(client, g['id'], g['title'], semaphore) for g in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    all_listings.extend(result)
            
            if i + batch_size < total_gifts:
                delay = random.uniform(config.BATCH_DELAY_MIN, config.BATCH_DELAY_MAX)
                if self.get_error_rate() > 0.15:
                    delay *= 1.5
                await asyncio.sleep(delay)
        
        return all_listings

    async def send_all_alerts_optimized(
        self,
        client: TelegramClient,
        chat_entity,
        listings: List[dict]
    ) -> None:
        if not listings:
            return
        
        logger.info(f"📤 Отправка {len(listings)} алертов")
        semaphore = asyncio.Semaphore(config.CONCURRENT_ALERTS)
        
        async def send_one(listing):
            async with semaphore:
                try:
                    if not self.check_circuit_breaker() or not await self.ensure_connected(client):
                        return
                    
                    raw_owner_id = listing.get('owner_id')
                    
                    # Attempt to check owner details
                    owner = await self.check_owner(client, raw_owner_id)
                    
                    # Fallback logic ensuring clickable link if ID exists
                    if not owner or owner == "Unknown/Hidden":
                        if raw_owner_id:
                            # Extract integer ID if it's an object
                            user_id = raw_owner_id.user_id if hasattr(raw_owner_id, 'user_id') else raw_owner_id
                            owner = f"[User {user_id}](tg://user?id={user_id})"
                        else:
                            owner = "Hidden"

                    link = f"https://t.me/nft/{listing['slug']}-{listing['number']}"
                    
                    price_text = ""
                    if listing.get('price'):
                         # Try to fetch amount from price object if it's complex, otherwise use directly
                         amount = getattr(listing['price'], 'amount', listing['price'])
                         price_text = f"\n💰 {amount} ⭐️"

                    msg = f"**{listing['title']}** `#{listing['number']}`{price_text}\n👤 {owner}\n{link}"
                    
                    await asyncio.sleep(random.uniform(1.5, 3.5))
                    
                    await self.safe_request(
                        client,
                        client.send_message,
                        chat_entity,
                        msg,
                        link_preview=True,
                        parse_mode='Markdown',
                        critical=False
                    )
                    self.stats['alerts'] += 1
                    
                    history_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'title': listing['title'],
                        'slug': listing['slug'],
                        'number': listing['number'],
                        'price': str(listing.get('price')),
                        'listing_id': listing['listing_id'],
                        'owner': owner,
                        'link': link
                    }
                    self.listings_history.append(history_entry)
                    
                    current_hour = datetime.now().strftime('%Y-%m-%d %H:00')
                    self.stats['hourly_alerts'][current_hour] = self.stats['hourly_alerts'].get(current_hour, 0) + 1
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки: {e}")
        
        await asyncio.gather(*[send_one(l) for l in listings], return_exceptions=True)
        logger.info(f"✓ Отправлено: {self.stats['alerts']} | Пропущено: {self.stats['skipped_no_owner']}")

    async def run(self):
        logger.info("=" * 60)
        logger.info("NFT MONITOR by wortexhf [SECURE MODE]")
        logger.info("=" * 60)
        logger.info(f"📁 Данные: {config.DATA_DIR}")
        logger.info("")
        
        self.load_stats()
        self.load_history()
        
        chat_entity = None
        keepalive = None
        health_monitor = None
        stats_saver = None
        
        try:
            await self.client.start()
            await asyncio.sleep(random.uniform(3, 6))
            me = await self.client.get_me()
            logger.info(f"✓ Подключено: {me.first_name}")
            self.health_status["connected"] = True
            
            keepalive = asyncio.create_task(self.keepalive_task(self.client))
            health_monitor = asyncio.create_task(self.health_monitor_task())
            stats_saver = asyncio.create_task(self.stats_saver_task())
            
            gifts = await self.get_available_gifts(self.client)
            if not gifts:
                logger.error("❌ Нет целевых подарков")
                return
            
            await asyncio.sleep(random.uniform(3, 6))
            
            try:
                chat_entity = await self.safe_request(self.client, self.client.get_entity, config.GROUP_INVITE, critical=True)
            except:
                chat_entity = await self.safe_request(self.client, self.client.get_entity, config.GROUP_ID, critical=True)
            
            logger.info(f"✓ Группа подключена")
            await asyncio.sleep(random.uniform(4, 8))
            
            logger.info("🔍 Начальное сканирование...")
            initial = await self.scan_all_gifts(self.client, gifts)
            
            now = datetime.now()
            for listing in initial:
                listing_id = listing['listing_id']
                self.seen_listings.add(listing_id)
                self.listing_timestamps[listing_id] = now
            
            logger.info(f"✓ База: {len(self.seen_listings)} листингов")
            logger.info(f"✓ Мониторинг запущен")
            logger.info(f"⚙ Безопасный режим:")
            logger.info(f"  - Все {len(gifts)} подарков каждый скан")
            logger.info(f"  - Интервал: {config.BASE_SCAN_INTERVAL[0]}-{config.BASE_SCAN_INTERVAL[1]}с")
            logger.info(f"  - Проверка владельцев: включена")
            logger.info(f"  - Автостоп: {config.CIRCUIT_BREAKER_THRESHOLD} ошибок")
            logger.info("")
            
            while True:
                try:
                    if not await self.ensure_connected(self.client):
                        await asyncio.sleep(60)
                        continue
                    
                    self.stats['scans'] += 1
                    scan_start = datetime.now()
                    
                    logger.info(f"{'='*60}")
                    logger.info(f"СКАН #{self.stats['scans']}")
                    logger.info(f"{'='*60}")
                    
                    all_listings = await self.scan_all_gifts(self.client, gifts)
                    logger.info(f"✓ Найдено {len(all_listings)} листингов")
                    
                    now = datetime.now()
                    current_listing_ids = {l['listing_id'] for l in all_listings}
                    new_listing_ids = current_listing_ids - self.seen_listings
                    
                    if new_listing_ids:
                        new_listings = [l for l in all_listings if l['listing_id'] in new_listing_ids]
                        logger.info(f"🆕 Новых: {len(new_listings)}")
                        
                        for listing_id in new_listing_ids:
                            self.seen_listings.add(listing_id)
                            self.listing_timestamps[listing_id] = now
                        
                        await self.send_all_alerts_optimized(self.client, chat_entity, new_listings)
                    else:
                        logger.info("⏸ Новых нет")
                    
                    self.cleanup_old()
                    self.update_health()
                    
                    error_rate = self.get_error_rate()
                    uptime = (datetime.now() - self.start_time).total_seconds()
                    
                    logger.info(f"📊 Статистика:")
                    logger.info(
                        f"  Алертов: {self.stats['alerts']} | "
                        f"Пропущено (no owner): {self.stats['skipped_no_owner']} | "
                        f"Кеш: {len(self.owner_cache)}"
                    )
                    logger.info(
                        f"  Успешно: {self.stats['successful_requests']} | "
                        f"Ошибок: {self.stats['failed_requests']} | "
                        f"Error rate: {error_rate:.1%}"
                    )
                    logger.info(
                        f"  FloodWait: {self.stats['flood_waits']} | "
                        f"Автостопов: {self.stats['circuit_breaks']} | "
                        f"Uptime: {uptime/3600:.1f}ч"
                    )
                    
                    scan_duration = (datetime.now() - scan_start).total_seconds()
                    delay_range = self.get_adaptive_delay()
                    delay = random.randint(*delay_range)
                    
                    if error_rate > 0.2:
                        logger.info(f"⚠ Увеличенная пауза")
                    
                    logger.info(f"⏸ Пауза {delay}с (скан {scan_duration:.1f}с)")
                    logger.info("")
                    
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка цикла: {e}")
                    self.stats['errors'] += 1
                    self.error_history.append((datetime.now(), True))
                    
                    delay = random.randint(60, 120)
                    logger.info(f"⏸ Восстановление {delay}с")
                    await asyncio.sleep(delay)
                    
                    try:
                        await self.ensure_connected(self.client)
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
            
            self.save_stats()
            
            try:
                with open(config.HISTORY_FILE, 'w', encoding='utf-8') as f:
                    json.dump(self.listings_history[-1000:], f, ensure_ascii=False, indent=2)
            except:
                pass
            
            try:
                await self.client.disconnect()
            except:
                pass
            
            logger.info("✓ Отключено")

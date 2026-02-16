import asyncio
import random
import traceback
import sys
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from collections import deque

from telethon import TelegramClient, events, types
from telethon.tl.custom import Button
from telethon.errors import (
    FloodWaitError, BadRequestError, RPCError, NetworkMigrateError, 
    PhoneMigrateError, TimedOutError, AuthKeyError
)
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest
from telethon.tl.functions.updates import GetStateRequest

import config
from utils import logger

BANNED_USERS_FILE = config.DATA_DIR / "banned_users.json"
TAKEN_USERS_FILE = config.DATA_DIR / "taken_users.json"
BOT_SESSION_PATH = config.DATA_DIR / "bot_session"

class NFTMonitor:
    def __init__(self):
        self.seen_listings: Set[str] = set()
        self.listing_timestamps: Dict[str, datetime] = {}
        self.owner_cache: Dict[int, Tuple[Optional[dict], datetime]] = {}
        self.banned_users: Set[int] = set()
        self.taken_users: Dict[str, str] = {} 
        self.last_request_times = deque(maxlen=50)
        self.error_history = deque(maxlen=100)
        self.circuit_breaker_until: Optional[datetime] = None
        self.consecutive_errors = 0
        self.health_status = {"connected": True, "last_success": datetime.now(), "error_rate": 0.0}
        self.start_time = datetime.now()
        
        self.alert_queue = asyncio.Queue()
        self.is_bootstrapping = True 
        
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
            timeout=60
        )
        
        self.bot_client = TelegramClient(
            str(BOT_SESSION_PATH),
            config.API_ID,
            config.API_HASH
        )

    def load_banned_users(self):
        try:
            if BANNED_USERS_FILE.exists():
                with open(BANNED_USERS_FILE, 'r', encoding='utf-8') as f:
                    self.banned_users = set(json.load(f))
                logger.info(f"✓ Загружено {len(self.banned_users)} забаненных пользователей")
        except Exception as e:
            logger.warning(f"Не удалось загрузить бан-лист: {e}")

    def save_banned_users(self):
        try:
            with open(BANNED_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.banned_users), f)
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения бан-листа: {e}")

    def load_taken_users(self):
        try:
            if TAKEN_USERS_FILE.exists():
                with open(TAKEN_USERS_FILE, 'r', encoding='utf-8') as f:
                    self.taken_users = json.load(f)
                logger.info(f"✓ Загружено {len(self.taken_users)} активных задач")
        except Exception as e:
            logger.warning(f"Не удалось загрузить список взятых задач: {e}")

    def save_taken_users(self):
        try:
            with open(TAKEN_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.taken_users, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения списка задач: {e}")

    async def handle_ban_callback(self, event):
        try:
            data = event.data.decode()
            if not data.startswith("ban_"): return
            user_id = int(data.split("_")[1])
            self.banned_users.add(user_id)
            self.save_banned_users()
            await event.answer("🚫 Пользователь заблокирован!", alert=True)
            try:
                msg = await event.get_message()
                new_text = msg.text + "\n\n🚫 **АВТОР ЗАБЛОКИРОВАН**"
                await msg.edit(new_text, buttons=None, link_preview=True)
            except: pass
        except Exception as e: logger.error(f"Ошибка в handle_ban_callback: {e}")

    async def handle_take_callback(self, event):
        try:
            data = event.data.decode()
            parts = data.split("_")
            if len(parts) < 2: return
            target_user_id = parts[1]
            sender = await event.get_sender()
            clicker_name = f"@{sender.username}" if sender.username else sender.first_name
            
            if target_user_id in self.taken_users:
                await event.answer(f"⚠️ Уже занято: {self.taken_users[target_user_id]}", alert=True)
                return

            self.taken_users[target_user_id] = clicker_name
            self.save_taken_users()
            await event.answer(f"✅ Ви взяли цього продавця!", alert=False)
            
            user_id_int = int(target_user_id)
            user_link = f"tg://user?id={user_id_int}"
            if user_id_int in self.owner_cache:
                user_data = self.owner_cache[user_id_int][0]
                if user_data and user_data.get('username'):
                    user_link = f"https://t.me/{user_data['username']}"

            try:
                msg = await event.get_message()
                new_text = msg.text + f"\n\n🔒 **Взял:** {clicker_name}"
                profile_btn = Button.url("🔗 Профиль", user_link)
                ban_btn = Button.inline("🚫 Заблокировать", data=f"ban_{target_user_id}".encode())
                await msg.edit(new_text, buttons=[[profile_btn], [ban_btn]], link_preview=True)
            except: pass
        except Exception as e: logger.error(f"Ошибка в handle_take_callback: {e}")

    def load_stats(self):
        try:
            if config.STATS_FILE.exists():
                with open(config.STATS_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    for key in ['scans', 'alerts', 'errors', 'skipped_no_owner', 'reconnects', 'flood_waits', 'circuit_breaks', 'successful_requests', 'failed_requests', 'total_listings_found']:
                        if key in loaded: self.stats[key] = loaded[key]
                    self.stats['unique_gifts_seen'] = set(loaded.get('unique_gifts_seen', []))
                    self.stats['hourly_alerts'] = loaded.get('hourly_alerts', {})
                    if 'start_time' in loaded:
                        self.start_time = datetime.fromisoformat(loaded['start_time'])
                        self.stats['start_time'] = loaded['start_time']
                    logger.info("✓ Статистика загружена")
        except Exception as e: logger.warning(f"Не удалось загрузить статистику: {e}")

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
        except Exception as e: logger.error(f"❌ Ошибка сохранения статистики: {e}")

    def load_history(self):
        try:
            if config.HISTORY_FILE.exists():
                with open(config.HISTORY_FILE, 'r', encoding='utf-8') as f:
                    self.listings_history = json.load(f)
                logger.info(f"✓ История загружена: {len(self.listings_history)} записей")
        except Exception as e: logger.warning(f"Не удалось загрузить историю: {e}")

    def get_error_rate(self) -> float:
        if not self.error_history: return 0.0
        recent_window = datetime.now() - timedelta(minutes=15)
        recent_errors = sum(1 for ts, is_error in self.error_history if ts > recent_window and is_error)
        total_recent = sum(1 for ts, _ in self.error_history if ts > recent_window)
        return recent_errors / max(total_recent, 1)

    def update_health(self):
        self.health_status["error_rate"] = self.get_error_rate()
        if self.stats['successful_requests'] > 0: self.health_status["last_success"] = datetime.now()

    def get_adaptive_delay(self) -> Tuple[int, int]:
        error_rate = self.get_error_rate()
        base_min, base_max = config.BASE_SCAN_INTERVAL
        mult = 2.5 if error_rate > 0.3 else 1.8 if error_rate > 0.15 else 1.0
        return (int(base_min * mult), int(base_max * mult))

    def cleanup_old(self):
        cutoff = datetime.now() - timedelta(hours=config.LISTING_MEMORY_HOURS)
        self.seen_listings = {k for k in self.seen_listings if self.listing_timestamps.get(k, datetime.now()) > cutoff}
        self.listing_timestamps = {k: v for k, v in self.listing_timestamps.items() if v > cutoff}
        cache_cutoff = datetime.now() - timedelta(hours=config.OWNER_CACHE_TTL_HOURS)
        self.owner_cache = {k: v for k, v in self.owner_cache.items() if v[1] > cache_cutoff}

    def check_circuit_breaker(self) -> bool:
        if self.circuit_breaker_until and datetime.now() < self.circuit_breaker_until: return False
        if self.circuit_breaker_until:
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

    async def adaptive_rate_limit(self):
        now = datetime.now()
        self.last_request_times.append(now)
        if len(self.last_request_times) >= 2:
            time_since_last = (now - self.last_request_times[-2]).total_seconds()
            min_delay = config.MIN_REQUEST_DELAY + (self.get_error_rate() * 4)
            if time_since_last < min_delay:
                await asyncio.sleep(min_delay - time_since_last + random.uniform(0.1, 0.3))

    async def safe_request(self, client: TelegramClient, request_func, *args, max_retries=config.MAX_RETRIES, critical=False, **kwargs):
        if not self.check_circuit_breaker():
            wait = (self.circuit_breaker_until - datetime.now()).total_seconds()
            if wait > 0: await asyncio.sleep(min(wait + 1, 15))
        
        for attempt in range(max_retries):
            try:
                await self.adaptive_rate_limit()
                result = await asyncio.wait_for(request_func(*args, **kwargs), timeout=config.REQUEST_TIMEOUT)
                self.stats['successful_requests'] += 1
                self.error_history.append((datetime.now(), False))
                self.consecutive_errors = 0
                return result
            except FloodWaitError as e:
                self.stats['flood_waits'] += 1
                wait_time = min(e.seconds + 30, 600)
                logger.warning(f"⏱ FloodWait {wait_time}с")
                self.trigger_circuit_breaker()
                await asyncio.sleep(wait_time)
            except (TimedOutError, asyncio.TimeoutError):
                logger.warning(f"⏱ Таймаут (попытка {attempt+1}/{max_retries})")
                if attempt == max_retries - 1 and critical: self.trigger_circuit_breaker()
                await asyncio.sleep(random.uniform(5, 10))
            except Exception as e:
                logger.error(f"❌ Ошибка RPC: {type(e).__name__}: {e}")
                if attempt == max_retries - 1 and critical: self.trigger_circuit_breaker()
                await asyncio.sleep(random.uniform(5, 10))
        return None

    async def ensure_connected(self, client: TelegramClient) -> bool:
        if not client.is_connected():
            try:
                await client.connect()
                await client.get_me()
                return True
            except: return False
        return True

    async def alert_worker(self):
        while True:
            listing = await self.alert_queue.get()
            try:
                await self.send_single_alert(listing)
                await asyncio.sleep(random.uniform(1.5, 3.0))
            except Exception as e:
                logger.error(f"Worker error: {e}")
            finally:
                self.alert_queue.task_done()

    async def get_available_gifts(self, client: TelegramClient) -> List[dict]:
        try:
            if not await self.ensure_connected(client): return []
            result = await self.safe_request(client, client, GetStarGiftsRequest(hash=0), critical=True)
            if not result or not hasattr(result, 'gifts'): return []
            gifts = [{'id': gift.id, 'title': gift.title} for gift in result.gifts if hasattr(gift, 'title') and gift.title in config.TARGET_GIFT_NAMES]
            logger.info(f"✓ Найдено {len(gifts)} целевых подарков")
            return gifts
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Ошибка получения подарков: {e}")
            return []

    async def check_owner(self, client: TelegramClient, owner_id) -> Optional[dict]:
        if not owner_id: return None
        uid = owner_id.user_id if hasattr(owner_id, 'user_id') else owner_id if isinstance(owner_id, int) else None
        if not uid: return None
        
        if uid in self.owner_cache:
            data, ts = self.owner_cache[uid]
            if datetime.now() - ts < timedelta(hours=config.OWNER_CACHE_TTL_HOURS): return data
        
        if not await self.ensure_connected(client): return None
        entity = await self.safe_request(client, client.get_entity, owner_id, max_retries=2)
        
        if not entity or not isinstance(entity, types.User) or entity.bot:
            self.owner_cache[uid] = (None, datetime.now())
            return None
        
        name = (entity.first_name or "Unknown") + (f" {entity.last_name}" if entity.last_name else "")
        user_data = {'id': uid, 'display_name': name.replace('[', '').replace(']', ''), 'username': entity.username}
        self.owner_cache[uid] = (user_data, datetime.now())
        return user_data

    async def fetch_and_process_listing(self, client: TelegramClient, gift_id: int, gift_name: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                if not self.check_circuit_breaker() or not await self.ensure_connected(client): return []
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                result = await self.safe_request(client, client, GetResaleStarGiftsRequest(
                    gift_id=gift_id, offset="", limit=config.FETCH_LIMIT, sort_by_num=False, sort_by_price=False
                ), critical=True)

                if not result or not hasattr(result, 'gifts'): return []

                for gift in result.gifts:
                    if not (hasattr(gift, 'num') and hasattr(gift, 'slug')): continue
                    listing_id = f"{gift.slug}-{gift.num}"
                    
                    if listing_id not in self.seen_listings:
                        listing_data = {
                            'title': gift_name, 'slug': gift.slug, 'number': gift.num,
                            'price': getattr(gift, 'price', None), 'owner_id': getattr(gift, 'owner_id', None),
                            'listing_id': listing_id,
                        }
                        self.seen_listings.add(listing_id)
                        self.listing_timestamps[listing_id] = datetime.now()
                        if not self.is_bootstrapping:
                            self.alert_queue.put_nowait(listing_data)
                return []
            except Exception as e:
                logger.debug(f"Ошибка загрузки {gift_name}: {e}")
                return []

    async def send_single_alert(self, listing: dict) -> None:
        try:
            raw_owner_id = listing.get('owner_id')
            if not raw_owner_id: return
            user_data = await self.check_owner(self.client, raw_owner_id)
            if not user_data or user_data['id'] in self.banned_users: return

            link = f"https://t.me/nft/{listing['slug']}-{listing['number']}"
            price = f"\n💰 {getattr(listing['price'], 'amount', listing['price'])} ⭐️" if listing.get('price') else ""
            msg = f"{link}\n\n🎁 **{listing['title']}** `#{listing['number']}`{price}\n👤 {user_data['display_name']}"
            
            u_link = f"https://t.me/{user_data['username']}" if user_data['username'] else f"tg://user?id={user_data['id']}"
            btns = [[Button.url("🔗 Профиль", u_link)], [Button.inline("👤 Взять в работу", data=f"take_{user_data['id']}"), Button.inline("🚫 Заблокировать", data=f"ban_{user_data['id']}")]]
            
            await self.safe_request(self.bot_client, self.bot_client.send_message, config.GROUP_ID, msg, link_preview=True, parse_mode='Markdown', buttons=btns)
            self.stats['alerts'] += 1
            self.listings_history.append({'timestamp': datetime.now().isoformat(), 'title': listing['title'], 'owner': user_data['display_name'], 'link': link})
        except Exception as e: logger.error(f"Alert error: {e}")

    async def scan_all_gifts(self, client: TelegramClient, gifts: List[dict]):
        shuffled = gifts.copy()
        random.shuffle(shuffled)
        semaphore = asyncio.Semaphore(2) 
        batch_size = 2
        total = len(shuffled)
        for i in range(0, total, batch_size):
            if not self.check_circuit_breaker(): break
            batch = shuffled[i:i+batch_size]
            logger.info(f"  > [{i+len(batch)}/{total}] Сканирование: {', '.join(g['title'] for g in batch)}")
            tasks = [self.fetch_and_process_listing(client, g['id'], g['title'], semaphore) for g in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(random.uniform(1.0, 2.0))

    async def run(self):
        logger.info("=" * 60)
        logger.info("NFT MONITOR by wortexhf [STABLE MODE]")
        logger.info("=" * 60)
        self.load_stats(); self.load_history(); self.load_banned_users(); self.load_taken_users()
        
        try:
            await self.client.start()
            await self.bot_client.start(bot_token=config.BOT_TOKEN)
            self.bot_client.add_event_handler(self.handle_ban_callback, events.CallbackQuery(pattern=b"ban_"))
            self.bot_client.add_event_handler(self.handle_take_callback, events.CallbackQuery(pattern=b"take_"))
            
            asyncio.create_task(self.alert_worker())
            
            gifts = await self.get_available_gifts(self.client)
            if not gifts: return
            
            logger.info(f"🔍 Начальное сканирование (0/{len(gifts)} категорий)...")
            self.is_bootstrapping = True
            await self.scan_all_gifts(self.client, gifts)
            self.is_bootstrapping = False
            logger.info(f"✓ База: {len(self.seen_listings)} листингов. Мониторинг запущен.")
            
            while True:
                if not await self.ensure_connected(self.client):
                    await asyncio.sleep(30); continue
                self.stats['scans'] += 1
                logger.info(f"СКАН #{self.stats['scans']} (Алертов: {self.stats['alerts']})")
                await self.scan_all_gifts(self.client, gifts)
                self.save_stats(); self.save_taken_users()
                await asyncio.sleep(random.randint(*self.get_adaptive_delay()))
        except Exception as e: logger.error(f"Критическая ошибка: {e}")
        finally:
            await self.client.disconnect(); await self.bot_client.disconnect()

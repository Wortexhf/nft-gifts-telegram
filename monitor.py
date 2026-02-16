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
from telethon.tl.functions.users import GetFullUserRequest

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
        
        self.is_bootstrapping = True 
        self.current_scan_found = 0
        
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
            config.SESSION_NAME, config.API_ID, config.API_HASH,
            connection_retries=5, retry_delay=8, auto_reconnect=True, timeout=60
        )
        self.bot_client = TelegramClient(str(BOT_SESSION_PATH), config.API_ID, config.API_HASH)

    def load_banned_users(self):
        try:
            if BANNED_USERS_FILE.exists():
                with open(BANNED_USERS_FILE, 'r', encoding='utf-8') as f:
                    self.banned_users = set(json.load(f))
                logger.info(f"✓ Загружено {len(self.banned_users)} забаненных пользователей")
        except: pass

    def save_banned_users(self):
        try:
            with open(BANNED_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.banned_users), f)
        except: pass

    def load_taken_users(self):
        try:
            if TAKEN_USERS_FILE.exists():
                with open(TAKEN_USERS_FILE, 'r', encoding='utf-8') as f:
                    self.taken_users = json.load(f)
                logger.info(f"✓ Загружено {len(self.taken_users)} активных задач")
        except: pass

    def save_taken_users(self):
        try:
            with open(TAKEN_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.taken_users, f, ensure_ascii=False, indent=2)
        except: pass

    async def handle_ban_callback(self, event):
        try:
            data = event.data.decode()
            if not data.startswith("ban_"): return
            user_id = int(data.split("_")[1])
            self.banned_users.add(user_id)
            self.save_banned_users()
            await event.answer("🚫 Пользователь заблокирован!", alert=True)
            msg = await event.get_message()
            await msg.edit(msg.text + "\n\n🚫 **АВТОР ЗАБЛОКИРОВАН**", buttons=None, link_preview=True)
        except: pass

    async def handle_take_callback(self, event):
        try:
            data = event.data.decode()
            parts = data.split("_")
            if len(parts) < 2: return
            target_user_id = parts[1]
            sender = await event.get_sender()
            clicker_name = f"@{sender.username}" if sender.username else sender.first_name
            if target_user_id in self.taken_users:
                await event.answer(f"⚠️ Уже занято: {self.taken_users[target_user_id]}", alert=True); return
            self.taken_users[target_user_id] = clicker_name
            self.save_taken_users()
            await event.answer(f"✅ Ви взяли цього продавця!")
            
            uid = int(target_user_id)
            btns = [[Button.inline("🔗 Профіль", data=f"prof_{uid}")], [Button.inline("🚫 Заблокировать", data=f"ban_{uid}")]]
            msg = await event.get_message()
            await msg.edit(msg.text + f"\n\n🔒 **Взял:** {clicker_name}", buttons=btns, link_preview=True)
        except: pass

    async def handle_prof_callback(self, event):
        """Logic from zrazok: send profile link to PM"""
        try:
            data = event.data.decode()
            if not data.startswith("prof_"): return
            uid = int(data.split("_")[1])
            u_link = f"tg://user?id={uid}"
            if uid in self.owner_cache:
                ud = self.owner_cache[uid][0]
                if ud and ud.get('username'): u_link = f"https://t.me/{ud['username']}"
            
            await event.answer(f"Профіль: {u_link}", alert=False)
            try:
                await self.bot_client.send_message(event.sender_id, f"👤 Посилання на продавця: {u_link}")
            except:
                await event.answer("❌ Будь ласка, спочатку запустіть бота в ЛС!", alert=True)
        except: pass

    def load_stats(self):
        try:
            if config.STATS_FILE.exists():
                with open(config.STATS_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    if 'scans' in loaded: self.stats['scans'] = loaded['scans']
                    logger.info("✓ Статистика загружена")
        except: pass

    def save_stats(self):
        try:
            st = self.stats.copy(); st['unique_gifts_seen'] = list(self.stats['unique_gifts_seen'])
            with open(config.STATS_FILE, 'w', encoding='utf-8') as f: json.dump(st, f, ensure_ascii=False, indent=2)
        except: pass

    def load_history(self):
        try:
            if config.HISTORY_FILE.exists():
                with open(config.HISTORY_FILE, 'r', encoding='utf-8') as f:
                    self.listings_history = json.load(f)
                logger.info(f"✓ История загружена")
        except: pass

    async def safe_request(self, client, func, *args, max_retries=3, **kwargs):
        for attempt in range(max_retries):
            try:
                res = await asyncio.wait_for(func(*args, **kwargs), timeout=30)
                return res
            except FloodWaitError as e:
                logger.warning(f"⏱ FloodWait {e.seconds}с"); await asyncio.sleep(e.seconds + 5)
            except Exception:
                await asyncio.sleep(2)
        return None

    async def check_owner(self, owner_id) -> Optional[dict]:
        uid = owner_id.user_id if hasattr(owner_id, 'user_id') else owner_id if isinstance(owner_id, int) else None
        if not uid: return None
        if uid in self.owner_cache:
            d, ts = self.owner_cache[uid]
            if datetime.now() - ts < timedelta(hours=12): return d
        
        try:
            entity = await self.client.get_entity(owner_id)
            if not isinstance(entity, types.User) or entity.bot:
                self.owner_cache[uid] = (None, datetime.now()); return None
            
            # Strict access check
            await self.client(GetFullUserRequest(entity))
            
            name = ((entity.first_name or "") + " " + (entity.last_name or "")).strip() or "Unknown"
            data = {'id': uid, 'name': name.replace('[', '').replace(']', ''), 'username': entity.username}
            self.owner_cache[uid] = (data, datetime.now())
            return data
        except:
            self.owner_cache[uid] = (None, datetime.now()); return None

    async def fetch_and_process(self, gift_id, gift_name, semaphore):
        async with semaphore:
            try:
                res = await self.safe_request(self.client, self.client, GetResaleStarGiftsRequest(
                    gift_id=gift_id, offset="", limit=config.FETCH_LIMIT, sort_by_num=False, sort_by_price=False
                ))
                if not res or not hasattr(res, 'gifts'): return
                for gift in res.gifts:
                    listing_id = f"{gift.slug}-{gift.num}"
                    if listing_id not in self.seen_listings:
                        self.seen_listings.add(listing_id)
                        self.listing_timestamps[listing_id] = datetime.now()
                        if not self.is_bootstrapping:
                            self.current_scan_found += 1
                            asyncio.create_task(self.immediate_alert(gift, gift_name))
            except: pass

    async def immediate_alert(self, gift, gift_name):
        sent_msg = None
        try:
            # 1. STRICT Peer Check (Skip channels immediately)
            if not (hasattr(gift, 'owner_id') and isinstance(gift.owner_id, types.PeerUser)):
                return 

            uid = gift.owner_id.user_id
            link = f"https://t.me/nft/{gift.slug}-{gift.num}"
            price = f"\n💰 {getattr(gift.price, 'amount', gift.price)} ⭐️" if hasattr(gift, 'price') and gift.price else ""
            
            # 2. Fast Send (Link first for preview)
            msg_text = f"{link}\n\n🎁 **{gift_name}** `#{gift.num}`{price}\n👤 Пошук продавця..."
            sent_msg = await self.bot_client.send_message(config.GROUP_ID, msg_text, link_preview=True)
            if not sent_msg: return

            # 3. Resolve Profile
            user_data = await self.check_owner(uid)
            
            # 4. Filter & Delete if inaccessible
            if not user_data or uid in self.banned_users:
                await self.bot_client.delete_messages(config.GROUP_ID, [sent_msg.id])
                return

            # 5. RICH UPDATE (Profile button as CALLBACK)
            final_text = f"{link}\n\n🎁 **{gift_name}** `#{gift.num}`{price}\n👤 {user_data['name']}"
            btns = [[Button.inline("🔗 Профіль", data=f"prof_{uid}")], [Button.inline("👤 Взять в работу", data=f"take_{uid}"), Button.inline("🚫 Заблокировать", data=f"ban_{uid}")]]
            
            await sent_msg.edit(final_text, buttons=btns, link_preview=True)
            self.stats['alerts'] += 1
        except:
            if sent_msg:
                try: await self.bot_client.delete_messages(config.GROUP_ID, [sent_msg.id])
                except: pass

    async def scan_all(self, gifts):
        random.shuffle(gifts)
        sem = asyncio.Semaphore(5); batch = 5
        for i in range(0, len(gifts), batch):
            logger.info(f"  > [{i+batch if i+batch<len(gifts) else len(gifts)}/{len(gifts)}] Сканінг...")
            tasks = [self.fetch_and_process(g['id'], g['title'], sem) for g in gifts[i:i+batch]]
            await asyncio.gather(*tasks)
            await asyncio.sleep(random.uniform(0.5, 1.0))

    async def run(self):
        logger.info("="*60 + "\nNFT MONITOR by wortexhf [ULTRA FAST]\n" + "="*60)
        self.load_stats(); self.load_history(); self.load_banned_users(); self.load_taken_users()
        try:
            await self.client.start(); await self.bot_client.start(bot_token=config.BOT_TOKEN)
            
            # Handlers
            self.bot_client.add_event_handler(self.handle_ban_callback, events.CallbackQuery(pattern=b"ban_"))
            self.bot_client.add_event_handler(self.handle_take_callback, events.CallbackQuery(pattern=b"take_"))
            self.bot_client.add_event_handler(self.handle_prof_callback, events.CallbackQuery(pattern=b"prof_"))
            
            gifts = [{'id': g.id, 'title': g.title} for g in (await self.client(GetStarGiftsRequest(hash=0))).gifts if g.title in config.TARGET_GIFT_NAMES]
            self.is_bootstrapping = True; await self.scan_all(gifts); self.is_bootstrapping = False
            logger.info(f"✓ База: {len(self.seen_listings)} листингов.")
            while True:
                self.stats['scans'] += 1; self.current_scan_found = 0
                await self.scan_all(gifts)
                if self.current_scan_found > 0: logger.info(f"🆕 Нових: {self.current_scan_found}")
                self.save_stats(); self.save_taken_users()
                await asyncio.sleep(random.randint(5, 10))
        except Exception as e: logger.error(f"Критична помилка: {e}")
        finally: await self.client.disconnect(); await self.bot_client.disconnect()

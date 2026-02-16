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
                logger.info(f"✓ Завантажено {len(self.banned_users)} забанених")
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
                logger.info(f"✓ Завантажено {len(self.taken_users)} активних задач")
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
            uid = int(data.split("_")[1])
            self.banned_users.add(uid)
            self.save_banned_users()
            logger.info(f"🚫 Користувача {uid} забанено")
            await event.answer("🚫 Користувача заблоковано!", alert=True)
            msg = await event.get_message()
            await msg.edit(msg.text + "\n\n🚫 **АВТОР ЗАБЛОКИРОВАН**", buttons=None, link_preview=True)
        except Exception as e: logger.error(f"Ban error: {e}")

    async def handle_take_callback(self, event):
        try:
            data = event.data.decode()
            if not data.startswith("take_"): return
            target_uid = data.split("_")[1]
            sender = await event.get_sender()
            clicker_name = f"@{sender.username}" if sender.username else sender.first_name
            
            if target_uid in self.taken_users:
                await event.answer(f"⚠️ Вже зайнято: {self.taken_users[target_uid]}", alert=True); return

            self.taken_users[target_uid] = clicker_name
            self.save_taken_users()
            logger.info(f"🔒 Взято в роботу: {target_uid} користувачем {clicker_name}")
            await event.answer(f"✅ Ви взяли цього продавця!")
            
            uid = int(target_uid)
            user_data = self.owner_cache.get(uid, (None, None))[0]
            
            # Form Profile button
            if user_data and user_data.get('username'):
                p_btn = Button.url("🔗 Профіль", f"https://t.me/{user_data['username']}")
            else:
                p_btn = Button.inline("🔗 Профіль", data=f"prof_{uid}".encode())
            
            msg = await event.get_message()
            btns = [[p_btn], [Button.inline("🚫 Заблокировать", data=f"ban_{uid}".encode())]]
            await msg.edit(msg.text + f"\n\n🔒 **Взяв:** {clicker_name}", buttons=btns, link_preview=True)
        except Exception as e: logger.error(f"Take error: {e}")

    async def handle_prof_callback(self, event):
        try:
            data = event.data.decode()
            if not data.startswith("prof_"): return
            uid = int(data.split("_")[1])
            logger.info(f"🔗 Запит профілю для {uid}")
            
            u_link = f"tg://user?id={uid}"
            if uid in self.owner_cache:
                ud = self.owner_cache[uid][0]
                if ud and ud.get('username'): u_link = f"https://t.me/{ud['username']}"
            
            await event.answer("✅ Посилання надіслано в особисті!", alert=False)
            try:
                sender = await event.get_sender()
                await self.bot_client.send_message(sender, f"👤 **Продавець:**\n{u_link}\n\n_Ви отримали це повідомлення, бо у продавця немає публічного імені (username)._", parse_mode='Markdown')
            except Exception as e:
                logger.error(f"Failed to send PM: {e}")
                await event.answer("❌ Бот не може написати вам! Спочатку натисніть 'Start' у самому боті.", alert=True)
        except Exception as e: logger.error(f"Prof error: {e}")

    def load_stats(self):
        try:
            if config.STATS_FILE.exists():
                with open(config.STATS_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    if 'scans' in loaded: self.stats['scans'] = loaded['scans']
                    logger.info("✓ Статистика завантажена")
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
                logger.info(f"✓ Історія завантажена")
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
            if not (hasattr(gift, 'owner_id') and isinstance(gift.owner_id, types.PeerUser)):
                return 

            uid = gift.owner_id.user_id
            link = f"https://t.me/nft/{gift.slug}-{gift.num}"
            price = f"\n💰 {getattr(gift.price, 'amount', gift.price)} ⭐️" if hasattr(gift, 'price') and gift.price else ""
            
            # 1. Fast Send
            msg_text = f"{link}\n\n🎁 **{gift_name}** `#{gift.num}`{price}\n👤 Пошук продавця..."
            sent_msg = await self.bot_client.send_message(config.GROUP_ID, msg_text, link_preview=True)
            if not sent_msg: return

            # 2. Resolve Profile
            user_data = await self.check_owner(uid)
            
            # 3. STRICT FILTER
            if not user_data or uid in self.banned_users:
                await self.bot_client.delete_messages(config.GROUP_ID, [sent_msg.id])
                return

            # 4. Profile Button Logic
            if user_data.get('username'):
                p_btn = Button.url("🔗 Профіль", f"https://t.me/{user_data['username']}")
            else:
                p_btn = Button.inline("🔗 Профіль", data=f"prof_{uid}".encode())

            final_text = f"{link}\n\n🎁 **{gift_name}** `#{gift.num}`{price}\n👤 {user_data['name']}"
            btns = [[p_btn], [Button.inline("👤 Взять в работу", data=f"take_{uid}".encode()), Button.inline("🚫 Заблокировать", data=f"ban_{uid}".encode())]]
            
            await sent_msg.edit(final_text, buttons=btns, link_preview=True)
            self.stats['alerts'] += 1
        except:
            if sent_msg:
                try: await self.bot_client.delete_messages(config.GROUP_ID, [sent_msg.id])
                except: pass

    async def scan_all(self, gifts):
        random.shuffle(gifts)
        sem = asyncio.Semaphore(10); batch = 5 
        for i in range(0, len(gifts), batch):
            logger.info(f"  > [{i+batch if i+batch<len(gifts) else len(gifts)}/{len(gifts)}] Сканування...")
            tasks = [self.fetch_and_process(g['id'], g['title'], sem) for g in gifts[i:i+batch]]
            await asyncio.gather(*tasks)
            await asyncio.sleep(random.uniform(0.3, 0.7))

    async def run(self):
        logger.info("="*60 + "\nNFT MONITOR by wortexhf [ULTRA FAST]\n" + "="*60)
        self.load_stats(); self.load_history(); self.load_banned_users(); self.load_taken_users()
        try:
            await self.client.start(); await self.bot_client.start(bot_token=config.BOT_TOKEN)
            
            # Re-register with explicit patterns
            self.bot_client.add_event_handler(self.handle_ban_callback, events.CallbackQuery(pattern=b"ban_"))
            self.bot_client.add_event_handler(self.handle_take_callback, events.CallbackQuery(pattern=b"take_"))
            self.bot_client.add_event_handler(self.handle_prof_callback, events.CallbackQuery(pattern=b"prof_"))
            
            gifts = [{'id': g.id, 'title': g.title} for g in (await self.client(GetStarGiftsRequest(hash=0))).gifts if g.title in config.TARGET_GIFT_NAMES]
            self.is_bootstrapping = True; await self.scan_all(gifts); self.is_bootstrapping = False
            logger.info(f"✓ База готова: {len(self.seen_listings)} листингов.")
            while True:
                self.stats['scans'] += 1; self.current_scan_found = 0
                await self.scan_all(gifts)
                if self.current_scan_found > 0: logger.info(f"🆕 Знайдено нових: {self.current_scan_found}")
                self.save_stats(); self.save_taken_users()
                await asyncio.sleep(random.randint(3, 7))
        except Exception as e: logger.error(f"Критична помилка: {e}")
        finally: await self.client.disconnect(); await self.bot_client.disconnect()

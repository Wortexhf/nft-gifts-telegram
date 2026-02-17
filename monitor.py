import asyncio
import random
import traceback
import sys
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
from collections import deque

from telethon import TelegramClient, events, types, functions
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

# Константы путей к файлам данных
BANNED_USERS_FILE = config.DATA_DIR / "banned_users.json"
TAKEN_USERS_FILE = config.DATA_DIR / "taken_users.json"
BOT_SESSION_PATH = config.DATA_DIR / "bot_instance"

class NFTMonitor:
    def __init__(self):
        # Инициализация хранилищ данных
        self.seen_listings: Set[str] = set()
        self.seen_authors: Dict[int, datetime] = {} 
        self.author_lock = asyncio.Lock()
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
        self.last_catalog_update = datetime.now() - timedelta(hours=1)
        self.last_cleanup = datetime.now()
        self.gifts = []
        
        self.is_bootstrapping = True 
        self.current_scan_found = 0
        
        # Сбор статистики
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
        
        # Инициализация клиентов
        self.client = TelegramClient(
            config.SESSION_NAME, config.API_ID, config.API_HASH,
            connection_retries=5, retry_delay=8, auto_reconnect=True, timeout=60
        )
        self.bot_client = TelegramClient(str(BOT_SESSION_PATH), config.API_ID, config.API_HASH)

    def cleanup_memory(self):
        """Очистка старых данных из оперативной памяти"""
        try:
            now = datetime.now()
            cutoff_listings = now - timedelta(hours=config.LISTING_MEMORY_HOURS)
            to_remove_listings = [lid for lid, ts in self.listing_timestamps.items() if ts < cutoff_listings]
            for lid in to_remove_listings:
                self.seen_listings.discard(lid)
                del self.listing_timestamps[lid]
            
            cutoff_authors = now - timedelta(hours=24)
            to_remove_authors = [uid for uid, ts in self.seen_authors.items() if ts < cutoff_authors]
            for uid in to_remove_authors:
                del self.seen_authors[uid]
                
            if to_remove_listings or to_remove_authors:
                logger.info(f"🧹 Очистка памяти: -{len(to_remove_listings)} лотов, -{len(to_remove_authors)} авторов")
            self.last_cleanup = now
        except: pass

    def load_banned_users(self):
        """Загрузка черного списка с принудительным преобразованием в int"""
        try:
            if BANNED_USERS_FILE.exists():
                with open(BANNED_USERS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Принудительно конвертируем все ID в int, чтобы избежать ошибок сравнения
                    self.banned_users = set(int(uid) for uid in data)
                logger.info(f"✓ Загружено {len(self.banned_users)} забаненных")
        except Exception as e:
            logger.error(f"Ошибка загрузки забаненных: {e}")

    def save_banned_users(self):
        """Сохранение черного списка"""
        try:
            with open(BANNED_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.banned_users), f)
        except: pass

    def load_taken_users(self):
        """Загрузка активных задач"""
        try:
            if TAKEN_USERS_FILE.exists():
                with open(TAKEN_USERS_FILE, 'r', encoding='utf-8') as f:
                    self.taken_users = json.load(f)
                logger.info(f"✓ Загружено {len(self.taken_users)} активных задач")
        except: pass

    def save_taken_users(self):
        """Сохранение активных задач"""
        try:
            with open(TAKEN_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.taken_users, f, ensure_ascii=False, indent=2)
        except: pass

    def load_stats(self):
        """Загрузка статистики"""
        try:
            if config.STATS_FILE.exists():
                with open(config.STATS_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    if 'scans' in loaded: self.stats['scans'] = loaded['scans']
                    logger.info("✓ Статистика загружена")
        except: pass

    def save_stats(self):
        """Сохранение статистики"""
        try:
            st = self.stats.copy()
            st['unique_gifts_seen'] = list(self.stats['unique_gifts_seen'])
            with open(config.STATS_FILE, 'w', encoding='utf-8') as f:
                json.dump(st, f, ensure_ascii=False, indent=2)
        except: pass

    def load_history(self):
        """Загрузка истории листингов"""
        try:
            if config.HISTORY_FILE.exists():
                with open(config.HISTORY_FILE, 'r', encoding='utf-8') as f:
                    self.listings_history = json.load(f)
                logger.info(f"✓ История загружена")
        except: pass

    def _clean_msg_text(self, text):
        """Очистка текста сообщения от системных статусов"""
        if not text: return ""
        text = re.sub(r'\n\n🚫 .*АВТОР ЗАБЛОКИРОВАН.*', '', text)
        text = re.sub(r'\n\n🔒 .*Взял:.*', '', text)
        return text.strip()

    async def handle_ban_callback(self, event):
        """Обработка кнопки блокировки"""
        try:
            data = event.data.decode()
            if not data.startswith("ban_"): return
            uid = int(data.split("_")[1])
            
            self.banned_users.add(uid)
            
            msg = await event.get_message()
            status_btn = None
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if btn.data and b"already_taken" in btn.data:
                            status_btn = btn
                            break
            
            new_buttons = []
            if status_btn: new_buttons.append([status_btn])
            new_buttons.append([Button.inline("✅ Разблокировать", data=f"unban_{uid}".encode())])
            
            final_text = self._clean_msg_text(msg.text) + "\n\n🚫 **АВТОР ЗАБЛОКИРОВАН**"
            
            await event.edit(final_text, buttons=new_buttons, link_preview=True)
            await event.answer("🚫 Пользователь заблокирован!", alert=True)
            
            self.save_banned_users()
            logger.info(f"🚫 Пользователь {uid} вручную добавлен в черный список.")
        except Exception as e:
            logger.error(f"Ошибка в handle_ban_callback: {e}")

    async def handle_unban_callback(self, event):
        """Обработка кнопки разблокировки"""
        try:
            data = event.data.decode()
            if not data.startswith("unban_"): return
            uid = int(data.split("_")[1])
            
            self.banned_users.discard(uid)
            
            msg = await event.get_message()
            status_btn = None
            if msg.buttons:
                for row in msg.buttons:
                    for btn in row:
                        if btn.data and b"already_taken" in btn.data:
                            status_btn = btn
                            break
            
            new_buttons = []
            if status_btn:
                new_buttons.append([status_btn])
            else:
                new_buttons.append([Button.inline("👤 Взять в работу", data=f"take_{uid}".encode())])
            
            new_buttons.append([Button.inline("🚫 Заблокировать", data=f"ban_{uid}".encode())])
            
            await event.edit(self._clean_msg_text(msg.text), buttons=new_buttons, link_preview=True)
            await event.answer("✅ Пользователь разблокирован!", alert=True)
            
            self.save_banned_users()
            logger.info(f"✅ Пользователь {uid} удален из черного списка.")
        except Exception as e:
            logger.error(f"Ошибка в handle_unban_callback: {e}")

    async def handle_status_callback(self, event):
        """Информационное окно для статусных кнопок"""
        await event.answer("🔒 Этот лот уже взят в работу кем-то из участников.", alert=True)

    async def handle_take_callback(self, event):
        """Обработка кнопки 'Взять в работу'"""
        try:
            data = event.data.decode()
            if not data.startswith("take_"): return
            uid_str = data.split("_")[1]
            uid_int = int(uid_str)
            sender = await event.get_sender()
            clicker_name = f"@{sender.username}" if sender.username else sender.first_name
            
            msg = await event.get_message()
            
            if uid_str in self.taken_users:
                taken_by = self.taken_users[uid_str]
                is_banned = uid_int in self.banned_users
                ban_btn = Button.inline("✅ Разблокировать", data=f"unban_{uid_str}".encode()) if is_banned else Button.inline("🚫 Заблокировать", data=f"ban_{uid_str}".encode())
                
                new_buttons = [[Button.inline(f"🔒 Занято: {taken_by}", data=b"already_taken")], [ban_btn]]
                await event.edit(buttons=new_buttons, link_preview=True)
                await event.answer(f"⚠️ Уже занято: {taken_by}", alert=True); return

            self.taken_users[uid_str] = clicker_name
            
            is_banned = uid_int in self.banned_users
            ban_btn = Button.inline("✅ Разблокировать", data=f"unban_{uid_str}".encode()) if is_banned else Button.inline("🚫 Заблокировать", data=f"ban_{uid_str}".encode())

            new_text = self._clean_msg_text(msg.text) + f"\n\n🔒 **Взял:** {clicker_name}"
            new_buttons = [
                [Button.inline(f"🔒 Взял: {clicker_name}", data=b"already_taken")],
                [Button.inline("🛑 Прекратить работу", data=f"stop_{uid_str}".encode())],
                [ban_btn]
            ]
            
            await event.edit(new_text, buttons=new_buttons, link_preview=True)
            await event.answer(f"✅ Вы взяли этого продавца!")
            
            self.save_taken_users()
            logger.info(f"✅ Продавец {uid_str} взят в работу пользователем {clicker_name}.")
        except Exception as e:
            logger.error(f"Ошибка в handle_take_callback: {e}")

    async def handle_stop_callback(self, event):
        """Обработка кнопки 'Прекратить работу'"""
        try:
            data = event.data.decode()
            if not data.startswith("stop_"): return
            uid_str = data.split("_")[1]
            uid_int = int(uid_str)
            sender = await event.get_sender()
            
            if uid_str not in self.taken_users:
                await event.answer("⚠️ Эта задача уже свободна.", alert=True); return
            
            taken_by = self.taken_users[uid_str]
            clicker_name = f"@{sender.username}" if sender.username else sender.first_name
            
            if taken_by != clicker_name:
                await event.answer(f"⚠️ Эту работу может прекратить только {taken_by}", alert=True); return

            del self.taken_users[uid_str]
            
            msg = await event.get_message()
            is_banned = uid_int in self.banned_users
            ban_btn = Button.inline("✅ Разблокировать", data=f"unban_{uid_str}".encode()) if is_banned else Button.inline("🚫 Заблокировать", data=f"ban_{uid_str}".encode())
            
            new_buttons = [
                [Button.inline("👤 Взять в работу", data=f"take_{uid_str}".encode())],
                [ban_btn]
            ]
            
            await event.edit(self._clean_msg_text(msg.text), buttons=new_buttons, link_preview=True)
            await event.answer("🛑 Работа прекращена. Лот снова свободен!", alert=True)
            
            self.save_taken_users()
            logger.info(f"🛑 Пользователь {clicker_name} прекратил работу с продавцом {uid_str}.")
        except Exception as e:
            logger.error(f"Ошибка в handle_stop_callback: {e}")

    async def handle_prof_callback(self, event):
        """Обработка отсутствующего юзернейма"""
        try:
            data = event.data.decode()
            if not data.startswith("prof_"): return
            await event.answer("⚠️ Юзернейм отсутствует. Зайдите в профиль через окно подарка!", alert=True)
        except Exception as e: logger.error(f"Ошибка профиля: {e}")

    async def handle_start(self, event):
        """Команда /start для получения ID чата"""
        logger.info(f"📩 Получено сообщение /start. ID этого чата: {event.chat_id}")
        await event.respond(f"👋 **Бот активирован!**\nID этого чата: `{event.chat_id}`\nСкопируйте его в .env, если сообщения не приходят.")

    async def check_owner(self, owner_id) -> Optional[dict]:
        """Получение информации о владельце"""
        uid = owner_id.user_id if hasattr(owner_id, 'user_id') else owner_id if isinstance(owner_id, int) else None
        if not uid: return None
        if uid in self.owner_cache:
            d, ts = self.owner_cache[uid]
            if datetime.now() - ts < timedelta(hours=12): return d
        
        try:
            entity = await self.client.get_entity(owner_id)
            if not isinstance(entity, types.User) or entity.bot:
                self.owner_cache[uid] = (None, datetime.now()); return None
            
            if getattr(entity, 'deleted', False) or getattr(entity, 'restricted', False):
                self.owner_cache[uid] = (None, datetime.now()); return None

            full = await self.client(GetFullUserRequest(entity))
            name = ((entity.first_name or "") + " " + (entity.last_name or "")).strip() or "Неизвестно"
            
            premium = getattr(entity, 'premium', False)
            price = None
            if hasattr(full.full_user, 'stars_rating') and full.full_user.stars_rating:
                price = getattr(full.full_user.stars_rating, 'message_price', None)

            if not entity.username and not entity.photo and not price:
                self.owner_cache[uid] = (None, datetime.now()); return None

            if not entity.username and not price:
                self.owner_cache[uid] = (None, datetime.now()); return None

            data = {
                'id': uid, 
                'name': name.replace('[', '').replace(']', ''), 
                'username': entity.username,
                'premium': premium,
                'price': price
            }
            self.owner_cache[uid] = (data, datetime.now())
            return data
        except:
            self.owner_cache[uid] = (None, datetime.now()); return None

    async def update_catalog(self, quiet=False):
        """Обновление типов подарков"""
        try:
            logger.info("📡 Обновление каталога подарков...")
            res = await self.client(GetStarGiftsRequest(hash=0))
            new_gifts = [{'id': g.id, 'title': g.title} for g in res.gifts if g.title in config.TARGET_GIFT_NAMES]
            self.gifts = new_gifts
            self.last_catalog_update = datetime.now()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления каталога: {e}")
            return False

    async def fetch_and_process(self, gift_id, gift_name, semaphore):
        """Процесс получения и обработки листингов"""
        async with semaphore:
            try:
                res = await self.client(GetResaleStarGiftsRequest(
                    gift_id=gift_id, offset="", limit=config.FETCH_LIMIT, sort_by_num=False, sort_by_price=False
                ))
                if not res or not hasattr(res, 'gifts'): return
                for gift in res.gifts:
                    listing_id = f"{gift.slug}-{gift.num}"
                    uid = gift.owner_id.user_id if hasattr(gift, 'owner_id') and isinstance(gift.owner_id, types.PeerUser) else None
                    
                    if listing_id in self.seen_listings:
                        if not self.is_bootstrapping: break 
                        if uid: self.seen_authors[uid] = datetime.now()
                        continue
                        
                    self.seen_listings.add(listing_id)
                    self.listing_timestamps[listing_id] = datetime.now()
                    
                    if not self.is_bootstrapping:
                        if uid:
                            async with self.author_lock:
                                if uid in self.seen_authors: continue
                                self.seen_authors[uid] = datetime.now()
                            
                            logger.info(f"🆕 Найден новый лот: {gift_name} #{gift.num}")
                            self.current_scan_found += 1
                            asyncio.create_task(self.immediate_alert(gift, gift_name, uid))
                        else:
                            logger.warning(f"⚠️ Лот {listing_id} не имеет owner_id")
                    else:
                        if uid: self.seen_authors[uid] = datetime.now()
            except FloodWaitError as e:
                logger.warning(f"⚠️ FLOOD: Ожидание {e.seconds}с для {gift_name}")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                logger.debug(f"Ошибка сканирования {gift_name}: {e}")

    async def immediate_alert(self, gift, gift_name, uid):
        """Отправка уведомления"""
        # 1. Жесткая проверка бана по ID (принудительно int)
        if int(uid) in self.banned_users:
            logger.info(f"🚫 Лот пропущен (автор {uid} в черном списке)")
            return

        sent_msg = None
        try:
            link = f"https://t.me/nft/{gift.slug}-{gift.num}"
            price_stars = f"💰 {getattr(gift.price, 'amount', gift.price)} ⭐️" if hasattr(gift, 'price') and gift.price else ""
            
            msg_text = f"🎁 **Обнаружен новый подарок на маркете**\n\n{link}\n\n🎁 **{gift_name}** `#{gift.num}`\n{price_stars}\n\n👤 Поиск продавца..."
            
            target_group = config.GROUP_ID
            try:
                target_group = await self.bot_client.get_input_entity(config.GROUP_ID)
            except:
                if isinstance(config.GROUP_ID, int) and str(config.GROUP_ID).startswith("-") and not str(config.GROUP_ID).startswith("-100"):
                    try: target_group = await self.bot_client.get_input_entity(int("-100" + str(config.GROUP_ID).lstrip("-")))
                    except: pass

            # Отправляем временное сообщение
            sent_msg = await self.bot_client.send_message(target_group, msg_text, link_preview=True)
            if not sent_msg: return

            # 2. Получаем данные владельца и ЕЩЕ РАЗ проверяем бан
            user_data = await self.check_owner(uid)
            if not user_data or int(uid) in self.banned_users:
                logger.info(f"🚫 Пропущено (бан или нет данных): {uid}")
                await self.bot_client.delete_messages(target_group, [sent_msg.id])
                return

            u_name = f"@{user_data['username']}" if user_data['username'] else user_data['name']
            u_mention = f"[{u_name}](tg://user?id={uid})"
            
            u_info = f"👤 **Продавец:** {u_mention} `[{uid}]`\n"
            u_info += f"⭐ **Статус:** {'Премиум' if user_data['premium'] else 'Обычный'}\n"
            if user_data['price']: u_info += f"💬 **Сообщения:** {user_data['price']} ⭐️"

            final_text = f"🎁 **Обнаружен новый подарок на маркете**\n\n{link}\n\n🎁 **{gift_name}** `#{gift.num}`\n{price_stars}\n\n{u_info}"
            
            # Вертикальное расположение кнопок для мобильных устройств
            btns = [
                [Button.inline("👤 Взять в работу", data=f"take_{uid}".encode())],
                [Button.inline("🚫 Заблокировать", data=f"ban_{uid}".encode())]
            ]
            
            await sent_msg.edit(final_text, buttons=btns, link_preview=True)
            logger.info(f"✅ Алерт отправлен: {gift_name} #{gift.num} для {u_name}")
            self.stats['alerts'] += 1
        except Exception as e:
            logger.error(f"Ошибка алерта: {e}")
            if sent_msg:
                try: await self.bot_client.delete_messages(target_group, [sent_msg.id])
                except: pass

    async def scan_all(self, gifts):
        """Цикл сканирования"""
        random.shuffle(gifts)
        sem = asyncio.Semaphore(10); batch = 5 
        start_time = datetime.now()
        for i in range(0, len(gifts), batch):
            current_batch = gifts[i:i+batch]
            batch_titles = ", ".join([g['title'].split()[-1] for g in current_batch])
            logger.info(f"  > [{i+len(current_batch)}/{len(gifts)}] Сканирование: {batch_titles}...")
            tasks = [self.fetch_and_process(g['id'], g['title'], semaphore=sem) for g in current_batch]
            # Исправлена ошибка: Semaphore передавался позиционно не туда
            await asyncio.gather(*tasks)
            await asyncio.sleep(random.uniform(0.3, 0.7))
        
        if datetime.now() - self.last_cleanup > timedelta(hours=1): self.cleanup_memory()

    async def run(self):
        """Запуск монитора"""
        logger.info("="*60 + "\nNFT MONITOR by wortexhf [ULTRA FAST]\n" + "="*60)
        self.load_stats(); self.load_history(); self.load_banned_users(); self.load_taken_users()
        try:
            await self.client.start(); await self.bot_client.start(bot_token=config.BOT_TOKEN)
            
            try:
                entity = await self.bot_client.get_entity(config.GROUP_ID)
                logger.info(f"📡 Бот подключен к: {getattr(entity, 'title', 'Чат')} (ID: {entity.id})")
            except: pass

            self.bot_client.add_event_handler(self.handle_ban_callback, events.CallbackQuery(pattern=re.compile(b"ban_.*")))
            self.bot_client.add_event_handler(self.handle_unban_callback, events.CallbackQuery(pattern=re.compile(b"unban_.*")))
            self.bot_client.add_event_handler(self.handle_take_callback, events.CallbackQuery(pattern=re.compile(b"take_.*")))
            self.bot_client.add_event_handler(self.handle_stop_callback, events.CallbackQuery(pattern=re.compile(b"stop_.*")))
            self.bot_client.add_event_handler(self.handle_status_callback, events.CallbackQuery(pattern=re.compile(b"already_taken")))
            self.bot_client.add_event_handler(self.handle_prof_callback, events.CallbackQuery(pattern=re.compile(b"prof_.*")))
            self.bot_client.add_event_handler(self.handle_start, events.NewMessage(pattern='/start'))
            
            await self.update_catalog(quiet=True)
            self.is_bootstrapping = True; await self.scan_all(self.gifts); self.is_bootstrapping = False
            logger.info(f"✓ База готова: {len(self.seen_listings)} листингов.")
            while True:
                if datetime.now() - self.last_catalog_update > timedelta(minutes=30): await self.update_catalog()
                self.stats['scans'] += 1; self.current_scan_found = 0
                await self.scan_all(self.gifts)
                if self.current_scan_found > 0: logger.info(f"🆕 Найдено новых: {self.current_scan_found}")
                self.save_stats(); self.save_taken_users()
                await asyncio.sleep(random.randint(3, 7))
        except Exception as e: logger.error(f"Критическая ошибка: {e}")
        finally: await self.client.disconnect(); await self.bot_client.disconnect()

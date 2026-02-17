import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Определяем путь к папке, где находится EXE или скрипт
if getattr(sys, 'frozen', False):
    SCRIPT_DIR = Path(sys.executable).parent
else:
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Явно загружаем .env
ENV_PATH = SCRIPT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

# Папка для данных
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Проверка обязательных полей
mandatory_fields = ['API_ID', 'API_HASH', 'BOT_TOKEN', 'GROUP_ID']
missing = [f for f in mandatory_fields if not os.getenv(f)]

if missing:
    print("="*50)
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА В .env ФАЙЛЕ!")
    print(f"Отсутствуют или закомментированы параметры: {', '.join(missing)}")
    print("Пожалуйста, откройте .env и заполните их.")
    print("="*50)
    sys.exit(1)

# Telegram Auth
API_ID = int(os.getenv('API_ID', 0))
API_HASH = os.getenv('API_HASH', '').strip('"\' ')
BOT_TOKEN = os.getenv('BOT_TOKEN', '').strip('"\' ')
GROUP_ID = int(os.getenv('GROUP_ID', 0))
GROUP_INVITE = os.getenv('GROUP_INVITE')

# Пути к файлам сессий (используем строки для Telethon)
SESSION_NAME = str(DATA_DIR / "nft_gift_monitor")
BOT_SESSION_PATH = str(DATA_DIR / "bot_instance")
LOG_FILE = DATA_DIR / "monitor.log"
STATS_FILE = DATA_DIR / "statistics.json"
HISTORY_FILE = DATA_DIR / "listings_history.json"
TOKEN_CACHE_FILE = DATA_DIR / "current_token.txt"

# ЛОГИКА СМЕНЫ БОТА: Очистка сессий при смене токена
try:
    if TOKEN_CACHE_FILE.exists():
        old_token = TOKEN_CACHE_FILE.read_text().strip()
        if old_token != BOT_TOKEN:
            print("🔄 Токен изменен. Очистка старых сессий...")
            for f in DATA_DIR.glob("*.session*"):
                try: f.unlink()
                except: pass
    TOKEN_CACHE_FILE.write_text(BOT_TOKEN)
except: pass

# Конфигурация мониторинга
default_gifts = [
    "Heart Locket", "Durov's Cap", "Precious Peach", "Heroic Helmet",
    "Perfume Bottle", "Magic Potion", "Nail Bracelet", "Mini Oscar",
    "Mighty Arm", "Ion Gem", "Gem Signet", "Artisan Brick",
    "Genie Lamp", "Electric Skull", "Sharp Tongue", "Bling Binky",
    "Bonded Ring", "Kissed Frog", "Loot Bag", "Scared Cat",
    "Westside Sign", "Neko Helmet", "Low Rider", "Signet Ring",
    "Astral Shard", "Swiss Watch", "Voodoo Doll"
]

env_gifts = os.getenv('TARGET_GIFT_NAMES')
if env_gifts:
    clean_gifts = env_gifts.replace('"', '').replace("'", "").replace('\n', ',')
    TARGET_GIFT_NAMES = [name.strip() for name in clean_gifts.split(',') if name.strip()]
else:
    TARGET_GIFT_NAMES = default_gifts

# Настройки агрессивности
BASE_SCAN_INTERVAL = (5, 10)
CONCURRENT_REQUESTS = 5
FETCH_LIMIT = 50
CONCURRENT_ALERTS = 10
LISTING_MEMORY_HOURS = 48
OWNER_CACHE_TTL_HOURS = 12
OWNER_CACHE_MAX_SIZE = 5000
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
KEEPALIVE_INTERVAL = 240
MIN_REQUEST_DELAY = 0.5           
MAX_REQUEST_DELAY = 1.5           
BATCH_DELAY_MIN = 1.0             
BATCH_DELAY_MAX = 3.0             
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_INTERVAL = 15
SAVE_STATS_INTERVAL = 60

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Пути
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

# Telegram Auth
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = str(SESSION_FILE.with_suffix(''))
GROUP_ID = int(os.getenv('GROUP_ID'))
GROUP_INVITE = os.getenv('GROUP_INVITE')
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Конфигурация мониторинга
TARGET_GIFT_NAMES = [
    "Heart Locket", "Durov's Cap", "Precious Peach", "Heroic Helmet",
    "Perfume Bottle", "Magic Potion", "Nail Bracelet", "Mini Oscar",
    "Mighty Arm", "Ion Gem", "Gem Signet", "Artisan Brick",
    "Genie Lamp", "Electric Skull", "Sharp Tongue", "Bling Binky",
    "Bonded Ring", "Kissed Frog", "Loot Bag", "Scared Cat",
    "Westside Sign", "Neko Helmet", "Low Rider", "Signet Ring",
    "Astral Shard", "Swiss Watch", "Voodoo Doll"
]

# === КОНФИГУРАЦИЯ АГРЕССИВНОГО РЕЖИМА ===
BASE_SCAN_INTERVAL = (5, 10)      # Снижено с (15, 30)
CONCURRENT_REQUESTS = 5           # Параллельные запросы
FETCH_LIMIT = 50                  # Глубина выборки (было 30), чтобы не пропускать лоты
CONCURRENT_ALERTS = 10            # Скорость отправки алертов

# Кэширование
LISTING_MEMORY_HOURS = 48
OWNER_CACHE_TTL_HOURS = 12        # Кэш владельцев (в часах)
OWNER_CACHE_MAX_SIZE = 5000

# Безопасность и повторы
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
KEEPALIVE_INTERVAL = 240

# Лимиты запросов (ускорено)
MIN_REQUEST_DELAY = 0.5           # Было 2.0 - минимальная пауза
MAX_REQUEST_DELAY = 1.5           # Было 5.0
BATCH_DELAY_MIN = 1.0             # Было 4.0 - пауза между пачками подарков
BATCH_DELAY_MAX = 3.0             # Было 8.0

# Предохранитель (Circuit Breaker)
CIRCUIT_BREAKER_THRESHOLD = 5     # Порог ошибок перед остановкой
CIRCUIT_BREAKER_TIMEOUT = 60      # Таймаут при блокировке (сек)
HEALTH_CHECK_INTERVAL = 15
SAVE_STATS_INTERVAL = 60

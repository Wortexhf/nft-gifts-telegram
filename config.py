import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Определяем путь к папке, где находится EXE или скрипт
if getattr(sys, 'frozen', False):
    # Если запущено как EXE
    SCRIPT_DIR = Path(sys.executable).parent
else:
    # Если запущено как скрипт
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Явно загружаем .env именно из этой папки
ENV_PATH = SCRIPT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# Папка для данных (сессии, логи, статистика)
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION_FILE = DATA_DIR / "nft_gift_monitor.session"
LOG_FILE = DATA_DIR / "monitor.log"
STATS_FILE = DATA_DIR / "statistics.json"
HISTORY_FILE = DATA_DIR / "listings_history.json"

# Telegram Auth (Данные API)
API_ID = int(os.getenv('API_ID') or 0)
API_HASH = os.getenv('API_HASH')
SESSION_NAME = str(SESSION_FILE.with_suffix(''))
GROUP_ID = int(os.getenv('GROUP_ID') or 0)
GROUP_INVITE = os.getenv('GROUP_INVITE')
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Конфигурация мониторинга
# По умолчанию используем полный список, если в .env пусто
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
    # Разделяем строку по запятой и убираем лишние пробелы
    TARGET_GIFT_NAMES = [name.strip() for name in env_gifts.split(',') if name.strip()]
else:
    TARGET_GIFT_NAMES = default_gifts

# === КОНФИГУРАЦИЯ АГРЕССИВНОГО РЕЖИМА ===
BASE_SCAN_INTERVAL = (5, 10)      # Интервал сканирования (сек)
CONCURRENT_REQUESTS = 5           # Параллельные запросы
FETCH_LIMIT = 50                  # Глубина выборки лотов
CONCURRENT_ALERTS = 10            # Скорость отправки уведомлений

# Кэширование
LISTING_MEMORY_HOURS = 48         # Сколько помнить лоты
OWNER_CACHE_TTL_HOURS = 12        # Кэш владельцев (в часах)
OWNER_CACHE_MAX_SIZE = 5000

# Безопасность и повторы
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
KEEPALIVE_INTERVAL = 240

# Лимиты запросов
MIN_REQUEST_DELAY = 0.5           
MAX_REQUEST_DELAY = 1.5           
BATCH_DELAY_MIN = 1.0             
BATCH_DELAY_MAX = 3.0             

# Предохранитель (Circuit Breaker)
CIRCUIT_BREAKER_THRESHOLD = 5     # Порог ошибок перед остановкой
CIRCUIT_BREAKER_TIMEOUT = 60      # Таймаут при блокировке (сек)
HEALTH_CHECK_INTERVAL = 15
SAVE_STATS_INTERVAL = 60

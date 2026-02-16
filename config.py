import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
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

# Monitoring Config
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

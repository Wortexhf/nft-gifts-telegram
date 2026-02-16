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

# === AGGRESSIVE MODE CONFIG ===
BASE_SCAN_INTERVAL = (5, 10)      # Reduced from (15, 30)
CONCURRENT_REQUESTS = 5           # Keep parallel requests
FETCH_LIMIT = 50                  # Fetch deeper (was 30) to catch items if we missed a cycle
CONCURRENT_ALERTS = 10            # Send alerts faster

# Caching
LISTING_MEMORY_HOURS = 48
OWNER_CACHE_TTL_HOURS = 12        # Cache owners longer to save requests
OWNER_CACHE_MAX_SIZE = 5000

# Safety & Retries
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
KEEPALIVE_INTERVAL = 240

# Rate Limiting (Tightened for speed)
MIN_REQUEST_DELAY = 0.5           # Was 2.0 - minimal pause between individual reqs
MAX_REQUEST_DELAY = 1.5           # Was 5.0
BATCH_DELAY_MIN = 1.0             # Was 4.0 - pause between batches of 3 gifts
BATCH_DELAY_MAX = 3.0             # Was 8.0

# Circuit Breaker (Safety net)
CIRCUIT_BREAKER_THRESHOLD = 5     # Allow a few more errors before stopping
CIRCUIT_BREAKER_TIMEOUT = 60      # Reduce timeout if we hit a wall (was 300)
HEALTH_CHECK_INTERVAL = 15
SAVE_STATS_INTERVAL = 60

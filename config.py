import os
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME")
GROUP_USERNAME = os.getenv("GROUP_USERNAME")

TARGET_GIFT_NAMES = [
    "Heart Locket", "Durov's Cap", "Precious Peach", "Heroic Helmet",
    "Perfume Bottle", "Magic Potion", "Nail Bracelet", "Mini Oscar",
    "Mighty Arm", "Ion Gem", "Gem Signet", "Artisan Brick",
    "Genie Lamp", "Electric Skull", "Sharp Tongue", "Bling Binky",
    "Bonded Ring", "Kissed Frog", "Loot Bag", "Scared Cat",
    "Westside Sign", "Neko Helmet", "Low Rider", "Signet Ring",
    "Astral Shard", "Swiss Watch", "Voodoo Doll"
]

SCAN_INTERVAL = (5, 8)
CONCURRENT_REQUESTS = 10
FETCH_LIMIT = 30
CONCURRENT_ALERTS = 25
LISTING_MEMORY_HOURS = 24
SKIP_NO_OWNER = True
MAX_USER_LEVEL = 3
OWNER_CACHE_MAX_SIZE = 1000
OWNER_CACHE_TRIM_SIZE = 600
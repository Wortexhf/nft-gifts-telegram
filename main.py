import asyncio
import sys
import traceback
import random
from monitor import NFTMonitor
from utils import logger

if __name__ == "__main__":
    try:
        while True:
            try:
                # Instantiate and run the monitor
                monitor = NFTMonitor()
                asyncio.run(monitor.run())
                logger.info("✓ Завершено")
                break
            except KeyboardInterrupt:
                logger.info("⏹ Остановлено")
                sys.exit(0)
            except EOFError:
                logger.error("❌ Нет сессии")
                sys.exit(1)
            except Exception as e:
                delay = random.randint(60, 120)
                logger.error(f"❌ Перезапуск через {delay}с")
                traceback.print_exc()
                asyncio.run(asyncio.sleep(delay))
    except KeyboardInterrupt:
        sys.exit(0)

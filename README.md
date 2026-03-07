# 🎁 Telegram NFT Gift Monitor

A professional tool for monitoring the secondary market of NFT gifts in Telegram. The bot tracks new listings and instantly sends notifications to your working group.

## ✨ Key Features

- 🚀 **Ultra-fast scanning**: Minimal delay between a listing appearing and an alert being sent.
- 👤 **Seller information**: Automatically retrieves data: Premium status, messaging price, and a direct profile link.
- 🛡️ **Smart filtering**: The bot ignores deleted accounts, "ghosts" (no photo or username), and blacklisted users.
- 👥 **Team collaboration**:
  - **"👤 Take on"** button: Marks a listing as being handled by you.
  - **"🛑 Release"** button: Frees up the listing if you changed your mind (only available to the task owner).
  - **"🚫 Ban"** button: Adds the seller to the blacklist directly from the chat.
- 📱 **Mobile-friendly**: Vertically arranged buttons for comfortable use on a phone.
- ⚙️ **External configuration**: API keys, tokens, and gift lists are configured via a `.env` file without touching the code.

## 🛠 Installation & Setup

1. **Preparation**:
   - Make a copy of the `.env.example` file and rename it `.env`.
   - Fill in your `API_ID`, `API_HASH` (from [my.telegram.org](https://my.telegram.org)) and `BOT_TOKEN` (from [@BotFather](https://t.me/BotFather)).
   - Set your `GROUP_ID` (must start with `-100`).

2. **Running with Python**:
   ```bash
   pip install -r requirements.txt
   python main.py
   ```

3. **Using the EXE version**:
   If you have a compiled file, simply place it next to your `.env` file and run it.

## 📝 Monitoring Configuration

In the `.env` file, use the `TARGET_GIFT_NAMES` parameter to list the gift names you want to track, separated by commas. For example:

```
TARGET_GIFT_NAMES = "Heart Locket, Durov's Cap, Astral Shard"
```

If left empty, the bot will search for all popular gift types by default.

## 📦 Building an EXE (Compilation)

If you want to run the monitor without installing Python, or share it with someone else while hiding the source code, you can compile it into a single executable:

1. Install PyInstaller:
   ```bash
   pip install pyinstaller
   ```
2. Run the build command:
   ```bash
   python -m PyInstaller --onefile --console --name NFT_Monitor main.py
   ```
3. The compiled file will appear in the `dist/` folder.

> **Important**: Place the generated `NFT_Monitor.exe` in the same folder as your `.env` file before running.

## 🗄 Data Storage

- All statistics, sessions, and blacklists are stored in the `data/` folder.
- **Important**: Do not delete `banned_users.json` if you want to keep your blacklist.
- **Changing the bot**: If you update `BOT_TOKEN` in `.env`, the program will automatically reset the old session on the next launch.
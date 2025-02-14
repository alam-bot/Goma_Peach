"""
Enhanced Telegram Bot Broadcast System
Features:
- Configurable delays for different target types
- Progress tracking
- FloodWait protection
"""

import asyncio
import logging
import sys
from typing import Optional, Union, Sequence, Any

from pyrogram import filters
from pyrogram.enums import ChatMembersFilter
from pyrogram.errors import FloodWait

from PARINDA import app
from PARINDA.misc import SUDOERS
from PARINDA.utils.database import (
    get_active_chats,
    get_authuser_names,
    get_client,
    get_served_chats,
    get_served_users,
)
from PARINDA.utils.decorators.language import language
from PARINDA.utils.formatters import alpha_to_int
from config import adminlist

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
IS_BROADCASTING = False

# Configuration for different broadcast types
BROADCAST_CONFIG = {
    "group": {
        "min_delay": 0.2,
        "max_delay": 2.0,
        "retry_delay": 5,
        "max_retries": 3,
        "flood_threshold": 200
    },
    "user": {
        "min_delay": 0.3,
        "max_delay": 2.5,
        "retry_delay": 5,
        "max_retries": 3,
        "flood_threshold": 200
    },
    "assistant": {
        "min_delay": 3.0,
        "max_delay": 6.0,
        "retry_delay": 10,
        "max_retries": 2,
        "flood_threshold": 300
    }
}

@app.on_message(filters.command("broadcast") & SUDOERS)
@language
async def broadcast_message(client, message, _):
    """Handle broadcast command with improved progress tracking and error handling"""
    global IS_BROADCASTING

    if IS_BROADCASTING:
        return await message.reply_text(_["broad_9"])

    x = message.reply_to_message.id if message.reply_to_message else None
    y = message.chat.id
    query = None

    if not message.reply_to_message:
        if len(message.command) < 2:
            return await message.reply_text(_["broad_2"])
        query = message.text.split(None, 1)[1]
        for opt in ["-pin", "-nobot", "-pinloud", "-assistant", "-user"]:
            query = query.replace(opt, "").strip()
        if not query:
            return await message.reply_text(_["broad_8"])

    IS_BROADCASTING = True
    status_msg = await message.reply_text(_["broad_1"])

    try:
        # Group broadcast
        if "-nobot" not in message.text:
            sent = 0
            pin = 0
            chats = [int(chat["chat_id"]) for chat in await get_served_chats()]
            total_chats = len(chats)

            for i in chats:
                try:
                    m = (
                        await app.forward_messages(i, y, x)
                        if message.reply_to_message
                        else await app.send_message(i, text=query)
                    )

                    if "-pin" in message.text or "-pinloud" in message.text:
                        try:
                            await m.pin(disable_notification="-pinloud" not in message.text)
                            pin += 1
                        except Exception as e:
                            logger.error(f"Pin failed for chat {i}: {e}")
                            continue

                    sent += 1
                    delay = BROADCAST_CONFIG["group"]["min_delay"] + (
                        sent / total_chats
                    ) * (
                        BROADCAST_CONFIG["group"]["max_delay"] -
                        BROADCAST_CONFIG["group"]["min_delay"]
                    )
                    await asyncio.sleep(delay)

                except FloodWait as fw:
                    logger.warning(f"FloodWait: {fw.value}s for chat {i}")
                    if fw.value > BROADCAST_CONFIG["group"]["flood_threshold"]:
                        continue
                    await asyncio.sleep(fw.value)

                except Exception as e:
                    logger.error(f"Failed to send to chat {i}: {e}")
                    retries = BROADCAST_CONFIG["group"]["max_retries"]
                    while retries > 0:
                        try:
                            await asyncio.sleep(BROADCAST_CONFIG["group"]["retry_delay"])
                            m = (
                                await app.forward_messages(i, y, x)
                                if message.reply_to_message
                                else await app.send_message(i, text=query)
                            )
                            sent += 1
                            break
                        except Exception as retry_e:
                            logger.error(f"Retry failed for chat {i}: {retry_e}")
                            retries -= 1

                if sent % 5 == 0:
                    try:
                        progress = (sent / total_chats) * 100
                        await status_msg.edit_text(
                            _["broad_3"].format(sent, pin) +
                            f"\nProgress: {progress:.1f}%"
                        )
                    except Exception as e:
                        logger.error(f"Failed to update progress: {e}")

        # User broadcast
        if "-user" in message.text:
            susr = 0
            users = [int(user["user_id"]) for user in await get_served_users()]
            total_users = len(users)

            for i in users:
                try:
                    m = (
                        await app.forward_messages(i, y, x)
                        if message.reply_to_message
                        else await app.send_message(i, text=query)
                    )
                    susr += 1
                    delay = BROADCAST_CONFIG["user"]["min_delay"] + (
                        susr / total_users
                    ) * (
                        BROADCAST_CONFIG["user"]["max_delay"] -
                        BROADCAST_CONFIG["user"]["min_delay"]
                    )
                    await asyncio.sleep(delay)

                except FloodWait as fw:
                    logger.warning(f"FloodWait: {fw.value}s for user {i}")
                    if fw.value > BROADCAST_CONFIG["user"]["flood_threshold"]:
                        continue
                    await asyncio.sleep(fw.value)

                except Exception as e:
                    logger.error(f"Failed to send to user {i}: {e}")
                    retries = BROADCAST_CONFIG["user"]["max_retries"]
                    while retries > 0:
                        try:
                            await asyncio.sleep(BROADCAST_CONFIG["user"]["retry_delay"])
                            m = (
                                await app.forward_messages(i, y, x)
                                if message.reply_to_message
                                else await app.send_message(i, text=query)
                            )
                            susr += 1
                            break
                        except Exception as retry_e:
                            logger.error(f"Retry failed for user {i}: {retry_e}")
                            retries -= 1

                if susr % 5 == 0:
                    try:
                        progress = (susr / total_users) * 100
                        await status_msg.edit_text(
                            _["broad_4"].format(susr) +
                            f"\nProgress: {progress:.1f}%"
                        )
                    except Exception as e:
                        logger.error(f"Failed to update progress: {e}")

        # Assistant broadcast with enhanced error handling
        if "-assistant" in message.text:
            try:
                if "CWMUSIC.core.userbot" not in sys.modules:
                    from CWMUSIC.core.userbot import assistants, initialize_userbot, get_active_assistants
                else:
                    from CWMUSIC.core.userbot import assistants, initialize_userbot, get_active_assistants

                status_msg = await message.reply_text(_["broad_5"])
                report = _["broad_6"]

                # Get only active assistants
                active_assistants = await get_active_assistants()
                if not active_assistants:
                    logger.warning("No active assistants found")
                    await status_msg.edit_text("⚠️ No active assistants available")
                    return

                for num in active_assistants:
                    sent = 0
                    failed = 0
                    skipped = 0
                    try:
                        # Initialize with custom session name
                        client = await initialize_userbot(
                            f"assistant_{num}_{message.chat.id}",  # Unique session name
                            api_id=app.api_id,
                            api_hash=app.api_hash,
                            assistant_id=num
                        )

                        if not client:
                            logger.error(f"Failed to initialize assistant {num}")
                            report += f"❌ Assistant {num}: Failed to initialize\n"
                            continue

                        # Get dialogs with error handling
                        try:
                            dialogs = []
                            async for dialog in client.get_dialogs():
                                # Only broadcast to groups, supergroups and channels
                                if dialog.chat and dialog.chat.id and dialog.chat.type in ["group", "supergroup", "channel"]:
                                    dialogs.append(dialog)
                                else:
                                    skipped += 1
                        except Exception as e:
                            logger.error(f"Failed to get dialogs for assistant {num}: {e}")
                            report += f"⚠️ Assistant {num}: Failed to get dialogs - {str(e)}\n"
                            continue

                        total_chats = len(dialogs)
                        if total_chats == 0:
                            report += f"ℹ️ Assistant {num}: No valid chats found (Skipped: {skipped})\n"
                            continue

                        # Process each dialog with rate limiting
                        rate_limit = 0  # Counter for rate limiting
                        for dialog in dialogs:
                            try:
                                if rate_limit >= 20:  # Reset after every 20 messages
                                    logger.info(f"Rate limit reached for assistant {num}, sleeping for 60s")
                                    await asyncio.sleep(60)
                                    rate_limit = 0

                                await client.forward_messages(
                                    dialog.chat.id, y, x
                                ) if message.reply_to_message else await client.send_message(
                                    dialog.chat.id, text=query
                                )
                                sent += 1
                                rate_limit += 1

                                # Update progress periodically
                                if sent % 5 == 0:
                                    progress = (sent / total_chats) * 100
                                    await status_msg.edit_text(
                                        f"{report}\n"
                                        f"📊 Assistant {num} Progress:\n"
                                        f"Progress: {progress:.1f}%\n"
                                        f"✅ Sent: {sent}\n"
                                        f"❌ Failed: {failed}\n"
                                        f"⏩ Skipped: {skipped}\n"
                                        f"⏳ Remaining: {total_chats - sent - failed}"
                                    )

                                # Enhanced delay for assistant broadcasts
                                await asyncio.sleep(BROADCAST_CONFIG["assistant"]["min_delay"])

                            except FloodWait as fw:
                                logger.warning(f"FloodWait: {fw.value}s for assistant {num}")
                                if fw.value > BROADCAST_CONFIG["assistant"]["flood_threshold"]:
                                    failed += 1
                                    continue
                                await asyncio.sleep(fw.value)
                                rate_limit = 0  # Reset rate limit after FloodWait

                            except Exception as e:
                                logger.error(f"Failed to send message in assistant {num}: {e}")
                                failed += 1
                                continue

                        report += (
                            f"📱 Assistant {num} Report:\n"
                            f"  ✅ Sent: {sent}\n"
                            f"  ❌ Failed: {failed}\n"
                            f"  ⏩ Skipped: {skipped}\n"
                            f"  📊 Success Rate: {(sent/(sent+failed))*100:.1f}%\n"
                        )

                    except Exception as e:
                        logger.error(f"Assistant {num} broadcast failed: {e}")
                        report += f"❌ Assistant {num}: Error - {str(e)}\n"
                        continue

                try:
                    await status_msg.edit_text(report)
                except Exception as e:
                    logger.error(f"Failed to update assistant broadcast report: {e}")
                    await message.reply_text("⚠️ Failed to update status, check logs for details")

            except ImportError:
                logger.error("Assistant module not available")
                await message.reply_text(
                    "❌ Assistant module not available. Please check your installation."
                )
            except Exception as e:
                error_msg = f"Assistant broadcast failed: {str(e)}"
                logger.error(error_msg)
                await message.reply_text(f"❌ {error_msg}")

    except Exception as e:
        error_msg = f"Broadcast failed: {str(e)}"
        logger.error(error_msg)
        await message.reply_text(error_msg)

    finally:
        IS_BROADCASTING = False

async def auto_clean():
    """Background task to clean up admin lists"""
    while not await asyncio.sleep(10):
        try:
            served_chats = await get_active_chats()
            for chat_id in served_chats:
                if chat_id not in adminlist:
                    adminlist[chat_id] = []
                    try:
                        async for user in app.get_chat_members(
                            chat_id, filter=ChatMembersFilter.ADMINISTRATORS
                        ):
                            if user.privileges.can_manage_video_chats:
                                adminlist[chat_id].append(user.user.id)

                        authusers = await get_authuser_names(chat_id)
                        for user in authusers:
                            user_id = await alpha_to_int(user)
                            if user_id:
                                adminlist[chat_id].append(user_id)

                    except Exception as e:
                        logger.error(f"Failed to update admin list for chat {chat_id}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Auto-clean error: {e}")
            continue

# Start background task
asyncio.create_task(auto_clean())

# -*- coding: utf-8 -*-
import os
import logging
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.contrib.middlewares.logging import LoggingMiddleware


log = logging.getLogger()
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d - [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
ch.setFormatter(formatter)
log.addHandler(ch)
log.propagate = False


bot_token = os.getenv('baintbot')
bot = Bot(token=bot_token)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())



@dp.message_handler(commands=['help', 'start'])
async def process_help_command(message: types.Message):
    space = '\t\t\t\t\t\t'
    await bot.send_message(
        message.chat['id'],
       f"⁠\n🎙{space}t.me/paintpotChanel{space}{space}{space}\n\n📫{space}t.me/paintpotGroup{space}{space}{space}\n\n🤖{space}@baintBot"
    )
    await bot.unpin_all_chat_messages(message.chat['id'])
    await bot.pin_chat_message(message.chat['id'], message.message_id + 1)


    log_row = [str(message.from_user['id']), str(message.from_user['first_name']), str(message.from_user['last_name']),
               str(message.from_user['username']), str(message.chat['id']), message.chat['type'],
               str(message.chat['title']), str(message.text), message.date]
    log.info(log_row)



#@dp.message_handler()
#async def echo(message: types.Message):
#    await message.reply(emojize('👷♂️'))

if __name__ == '__main__':
    executor.start_polling(dp)


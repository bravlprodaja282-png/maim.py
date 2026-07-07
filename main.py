import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any, Awaitable, Callable

from sqlalchemy import BigInteger, String, Integer, Boolean, DateTime, ForeignKey, Text, select, update, func, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, joinedload

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, TelegramObject, ReplyKeyboardMarkup,
    KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.filters import Command

# ==========================================
# 1. КОНФИГУРАЦИЯ И ЛОГИРОВАНИЕ
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ЗАМЕНИТЕ НА СВОЙ НОВЫЙ ТОКЕН ИЗ @BotFather!
BOT_TOKEN = "8595365456:AAFgwudBkyxqrR9phLZfUyrtqaKgDRkJB88"
FOUNDER_PASSWORD = "milka"
DB_URL = "sqlite+aiosqlite:///tg_bot.db"

# ==========================================
# 2. БАЗА ДАННЫХ И МОДЕЛИ (SQLAlchemy)
# ==========================================
engine = create_async_engine(url=DB_URL, echo=False)
async_session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = 'users'
    tg_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    gender: Mapped[str] = mapped_column(String(10), nullable=False)
    age: Mapped[int] = mapped_column(Integer, nullable=False)
    premium_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=None)
    reg_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    chat_count: Mapped[int] = mapped_column(Integer, default=0)
    complaint_count: Mapped[int] = mapped_column(Integer, default=0)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    lang: Mapped[str] = mapped_column(String(5), default='ru')
    role: Mapped[str] = mapped_column(String(20), default='user')

    settings: Mapped["UserSettings"] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="joined")

    @property
    def is_premium(self) -> bool:
        if self.premium_until is None:
            return False
        return self.premium_until > datetime.now()


class UserSettings(Base):
    __tablename__ = 'user_settings'
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('users.tg_id', ondelete='CASCADE'), primary_key=True)
    allow_photo: Mapped[bool] = mapped_column(Boolean, default=True)
    allow_video: Mapped[bool] = mapped_column(Boolean, default=True)
    allow_voice: Mapped[bool] = mapped_column(Boolean, default=True)
    allow_documents: Mapped[bool] = mapped_column(Boolean, default=True)
    notifications: Mapped[bool] = mapped_column(Boolean, default=True)

    user: Mapped["User"] = relationship(back_populates="settings")


class Dialog(Base):
    __tablename__ = 'dialogs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user1_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user2_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    start_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    end_time: Mapped[datetime] = mapped_column(DateTime, nullable=True)


class Complaint(Base):
    __tablename__ = 'complaints'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    from_tg_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    to_tg_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class LogAction(Base):
    __tablename__ = 'logs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, nullable=True)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with engine.connect() as conn:
        try:
            result = await conn.execute(text("PRAGMA table_info(users);"))
            columns = [row[1] for row in result.fetchall()]

            if "premium_until" not in columns:
                logging.info("🔧 Обнаружена старая структура базы данных. Добавляю колонку premium_until...")
                await conn.execute(text("ALTER TABLE users ADD COLUMN premium_until DATETIME DEFAULT NULL;"))
                await conn.commit()
                logging.info("✅ Колонка premium_until успешно внедрена!")
        except Exception as e:
            logging.error(f"Не удалось выполнить автомиграцию: {e}")


# ==========================================
# 3. МАТЧМЕЙКИНГ (ОЧЕРЕДИ СИНГЛТОН)
# ==========================================
class SearchUser:
    def __init__(self, tg_id: int, gender: str, age: int, is_premium: bool,
                 search_gender: Optional[str] = None, age_min: Optional[int] = None, age_max: Optional[int] = None):
        self.tg_id = tg_id
        self.gender = gender
        self.age = age
        self.is_premium = is_premium
        self.search_gender = search_gender if search_gender in ["M", "F"] else None
        self.age_min = age_min or 12
        self.age_max = age_max or 99


class Matchmaker:
    def __init__(self):
        self.regular_queue: Dict[int, SearchUser] = {}
        self.adult_queue: Dict[int, SearchUser] = {}
        self.active_chats: Dict[int, int] = {}
        self._lock = asyncio.Lock()

    async def add_to_queue(self, user: SearchUser, is_adult_mode: bool = False) -> Optional[int]:
        async with self._lock:
            target_queue = self.adult_queue if is_adult_mode else self.regular_queue
            self.regular_queue.pop(user.tg_id, None)
            self.adult_queue.pop(user.tg_id, None)

            match_id = self._find_match(user, target_queue)
            if match_id:
                target_queue.pop(match_id, None)
                self.active_chats[user.tg_id] = match_id
                self.active_chats[match_id] = user.tg_id
                return match_id
            else:
                target_queue[user.tg_id] = user
                return None

    def _find_match(self, current_user: SearchUser, queue: Dict[int, SearchUser]) -> Optional[int]:
        sorted_candidates = sorted(queue.values(), key=lambda u: u.is_premium, reverse=True)
        for candidate in sorted_candidates:
            if candidate.tg_id == current_user.tg_id:
                continue
            if current_user.is_premium:
                if current_user.search_gender and candidate.gender != current_user.search_gender:
                    continue
                if not (current_user.age_min <= candidate.age <= current_user.age_max):
                    continue
            if candidate.is_premium:
                if candidate.search_gender and current_user.gender != candidate.search_gender:
                    continue
                if not (candidate.age_min <= current_user.age <= candidate.age_max):
                    continue
            return candidate.tg_id
        return None

    async def remove_from_queue(self, tg_id: int):
        async with self._lock:
            self.regular_queue.pop(tg_id, None)
            self.adult_queue.pop(tg_id, None)

    async def close_chat(self, tg_id: int) -> Optional[int]:
        async with self._lock:
            partner_id = self.active_chats.pop(tg_id, None)
            if partner_id:
                self.active_chats.pop(partner_id, None)
            return partner_id

    def get_partner_id(self, tg_id: int) -> Optional[int]:
        return self.active_chats.get(tg_id)

    def get_stats(self) -> Tuple[int, int]:
        return len(self.regular_queue) + len(self.adult_queue), len(self.active_chats) // 2


matchmaker = Matchmaker()


# ==========================================
# 4. СОСТОЯНИЯ FSM
# ==========================================
class RegStates(StatesGroup):
    rules = State()
    gender = State()
    age = State()


class ChatStates(StatesGroup):
    menu = State()
    searching = State()
    in_chat = State()


# ==========================================
# 5. СРЕДСТВА ЗАЩИТЫ (MIDDLEWARES)
# ==========================================
class AntiFloodMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 0.7):
        self.limit = limit
        self.storage: Dict[int, float] = {}
        super().__init__()

    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]], event: TelegramObject,
                       data: Dict[str, Any]) -> Any:
        if not isinstance(event, (Message, CallbackQuery)):
            return await handler(event, data)
        user_id = event.from_user.id
        current_time = time.time()
        if current_time - self.storage.get(user_id, 0.0) < self.limit:
            if isinstance(event, Message):
                await event.answer("⚠️ Пожалуйста, не флудите.")
            return
        self.storage[user_id] = current_time
        return await handler(event, data)


class AuthMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]], event: TelegramObject,
                       data: Dict[str, Any]) -> Any:
        if not isinstance(event, (Message, CallbackQuery)):
            return await handler(event, data)
        user_id = event.from_user.id
        async with async_session() as session:
            res = await session.execute(
                select(User)
                .where(User.tg_id == user_id)
                .options(joinedload(User.settings))
            )
            db_user = res.scalars().first()

            if db_user and db_user.is_banned:
                if isinstance(event, Message):
                    await event.answer("❌ Вы заблокированы за нарушение правил.")
                return
            data["db_user"] = db_user
        return await handler(event, data)


# ==========================================
# 6. КЛАВИАТУРЫ (KEYBOARDS)
# ==========================================
def kb_main_menu():
    # ИСПРАВЛЕНО: Добавлена кнопка "Пошлый чат (18+)" в разметку основного меню
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔍 Найти собеседника"), KeyboardButton(text="🔞 Пошлый чат (18+)")],
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="⚙ Настройки")],
        [KeyboardButton(text="📜 Правила"), KeyboardButton(text="🆘 Поддержка")],
        [KeyboardButton(text="⭐ Premium"), KeyboardButton(text="💎 Купить Premium")]
    ], resize_keyboard=True)


def kb_chat():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="❌ Завершить чат"), KeyboardButton(text="🔄 Следующий собеседник")],
        [KeyboardButton(text="🚩 Пожаловаться")]
    ], resize_keyboard=True)


def kb_search():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🛑 Отмена поиска")]], resize_keyboard=True)


# ==========================================
# 7. ХЭНДЛЕРЫ ЛОГИКИ
# ==========================================
router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, db_user: Optional[User]):
    if db_user:
        await state.set_state(ChatStates.menu)
        await message.answer("👋 Добро пожаловать обратно в главное меню!", reply_markup=kb_main_menu())
    else:
        await state.set_state(RegStates.rules)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Принимаю правила", callback_data="accept_rules")]])
        await message.answer(
            "Добро пожаловать в анонимный чат! 🛑 Перед началом вы должны принять правила.\n\nЗапрещен спам, оскорбления, порнография и реклама.",
            reply_markup=kb)


@router.callback_query(RegStates.rules, F.data == "accept_rules")
async def process_rules(callback: CallbackQuery, state: FSMContext):
    await state.set_state(RegStates.gender)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🙋‍♂️ Мужчина", callback_data="gender_M"),
         InlineKeyboardButton(text="🙋‍♀️ Женщина", callback_data="gender_F")]
    ])
    await callback.message.edit_text("Отлично! Выберите ваш пол:", reply_markup=kb)
    await callback.answer()


@router.callback_query(RegStates.gender, F.data.startswith("gender_"))
async def process_gender(callback: CallbackQuery, state: FSMContext):
    gender = callback.data.split("_")[1]
    await state.update_data(gender=gender)
    await state.set_state(RegStates.age)
    await callback.message.edit_text("Введите ваш возраст (числом от 12 до 99):")
    await callback.answer()


@router.message(RegStates.age)
async def process_age(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (12 <= int(message.text) <= 99):
        await message.answer("Пожалуйста, введите корректный возраст (только цифры, от 12 до 99):")
        return
    age = int(message.text)
    data = await state.get_data()

    async with async_session() as session:
        new_user = User(tg_id=message.from_user.id, gender=data["gender"], age=age)
        new_settings = UserSettings(user_id=message.from_user.id)
        session.add(new_user)
        session.add(new_settings)
        log = LogAction(tg_id=message.from_user.id, action=f"Регистрация: пол {data['gender']}, возраст {age}")
        session.add(log)
        await session.commit()

    await state.set_state(ChatStates.menu)
    await message.answer("🎉 Регистрация успешно завершена!", reply_markup=kb_main_menu())


# --- ПРОФИЛЬ, НАСТРОЙКИ, ПРАВИЛА ---
@router.message(F.text == "👤 Профиль")
async def view_profile(message: Message, db_user: Optional[User]):
    if not db_user: return
    g = "Мужской" if db_user.gender == "M" else "Женский"

    if db_user.is_premium:
        if db_user.premium_until and db_user.premium_until.year > 2090:
            prem = "✅ Навсегда"
        else:
            prem = f"✅ До {db_user.premium_until.strftime('%d.%m.%Y %H:%M')}"
    else:
        prem = "❌ Отсутствует"

    profile_text = (f"👤 **Ваш профиль:**\n\n"
                    f" Пол: {g}\n"
                    f" Возраст: {db_user.age}\n"
                    f" Premium: {prem}\n"
                    f" Всего чатов: {db_user.chat_count}\n"
                    f" Жалоб на вас: {db_user.complaint_count}\n"
                    f" Статус: {db_user.role.upper()}")
    await message.answer(profile_text, parse_mode="Markdown")


@router.message(F.text == "📜 Правила")
async def view_rules(message: Message):
    await message.answer(
        "📜 **Правила сервиса:**\n1. Запрещены оскорбления.\n2. Никакого спама и рекламы.\n3. Запрещена отправка порнографических материалов.\nЗа нарушение — мгновенный вечный бан.")


@router.message(F.text == "🆘 Поддержка")
async def view_support(message: Message):
    await message.answer(
        "🆘 По всем вопросам, багам или предложениям обращайтесь к администратору: @solnvox")


@router.message(F.text == "⚙ Настройки")
async def view_settings(message: Message, db_user: Optional[User]):
    if not db_user: return
    s = db_user.settings

    def check(val): return "✅" if val else "❌"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Фото: {check(s.allow_photo)}", callback_data="toggle_photo")],
        [InlineKeyboardButton(text=f"Видео: {check(s.allow_video)}", callback_data="toggle_video")],
        [InlineKeyboardButton(text=f"Голосовые: {check(s.allow_voice)}", callback_data="toggle_voice")],
        [InlineKeyboardButton(text=f"Документы: {check(s.allow_documents)}", callback_data="toggle_documents")],
        [InlineKeyboardButton(text=f"Уведомления: {check(s.notifications)}", callback_data="toggle_notif")]
    ])
    await message.answer("⚙ Настройки получения медиаконтента (Для Premium можно отключать):", reply_markup=kb)


@router.callback_query(F.data.startswith("toggle_"))
async def process_toggle_settings(callback: CallbackQuery, db_user: Optional[User]):
    if not db_user: return
    if not db_user.is_premium:
        await callback.answer("⭐ Изменение настроек медиа доступно только Premium пользователям!", show_alert=True)
        return

    field = callback.data.replace("toggle_", "")
    s = db_user.settings

    async with async_session() as session:
        session.add(s)
        if field == "photo":
            s.allow_photo = not s.allow_photo
        elif field == "video":
            s.allow_video = not s.allow_video
        elif field == "voice":
            s.allow_voice = not s.allow_voice
        elif field == "documents":
            s.allow_documents = not s.allow_documents
        elif field == "notif":
            s.notifications = not s.notifications
        await session.commit()

    def check(val):
        return "✅" if val else "❌"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Фото: {check(s.allow_photo)}", callback_data="toggle_photo")],
        [InlineKeyboardButton(text=f"Видео: {check(s.allow_video)}", callback_data="toggle_video")],
        [InlineKeyboardButton(text=f"Голосовые: {check(s.allow_voice)}", callback_data="toggle_voice")],
        [InlineKeyboardButton(text=f"Документы: {check(s.allow_documents)}", callback_data="toggle_documents")],
        [InlineKeyboardButton(text=f"Уведомления: {check(s.notifications)}", callback_data="toggle_notif")]
    ])
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer("Настройки обновлены.")


# --- ПОИСК И СИСТЕМА ЧАТА ---
async def start_search_logic(message: Message, state: FSMContext, db_user: User, search_gender: Optional[str] = None,
                             is_adult: bool = False):
    await state.set_state(ChatStates.searching)
    await message.answer("🔍 Ищем подходящего собеседника...", reply_markup=kb_search())

    s_user = SearchUser(
        tg_id=db_user.tg_id,
        gender=db_user.gender,
        age=db_user.age,
        is_premium=db_user.is_premium,
        search_gender=search_gender
    )
    partner_id = await matchmaker.add_to_queue(s_user, is_adult_mode=is_adult)

    if partner_id:
        async with async_session() as session:
            d = Dialog(user1_id=db_user.tg_id, user2_id=partner_id)
            session.add(d)
            await session.execute(update(User).where(User.tg_id.in_([db_user.tg_id, partner_id])).values(
                chat_count=User.chat_count + 1))
            await session.commit()

        await state.set_state(ChatStates.in_chat)
        from_bot = message.bot

        partner_storage_key = StorageKey(bot_id=from_bot.id, chat_id=partner_id, user_id=partner_id)
        partner_state = FSMContext(storage=state.storage, key=partner_storage_key)
        await partner_state.set_state(ChatStates.in_chat)

        text_success = "🤝 Собеседник найден! Напишите приветствие. Используйте меню для управления."

        await message.answer(text_success, reply_markup=kb_chat())
        try:
            await from_bot.send_message(partner_id, text_success, reply_markup=kb_chat())
        except Exception as e:
            logging.error(f"Не удалось отправить уведомление о старте чата партнеру {partner_id}: {e}")


@router.message(F.text == "🔍 Найти собеседника")
async def cmd_search_menu(message: Message, db_user: Optional[User]):
    if not db_user: return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🙋‍♂️ Искать парня", callback_data="search_gender_M")],
        [InlineKeyboardButton(text="🙋‍♀️ Искать девушку", callback_data="search_gender_F")],
        [InlineKeyboardButton(text="🌍 Всё равно (Любой пол)", callback_data="search_gender_ALL")]
    ])

    await message.answer("Ккого вы хотите найти?", reply_markup=kb)


@router.callback_query(F.data.startswith("search_gender_"))
async def process_search_gender(callback: CallbackQuery, state: FSMContext, db_user: Optional[User]):
    if not db_user: return
    target = callback.data.replace("search_gender_", "")

    if target in ["M", "F"] and not db_user.is_premium:
        await callback.answer("⭐ Поиск по полу доступен только Premium пользователям! Запускаю обычный поиск...",
                              show_alert=True)
        target = "ALL"
    else:
        await callback.answer()

    await callback.message.delete()
    search_sex = None if target == "ALL" else target
    await start_search_logic(callback.message, state, db_user, search_gender=search_sex, is_adult=False)


@router.message(ChatStates.searching, F.text == "🛑 Отмена поиска")
async def cancel_search(message: Message, state: FSMContext):
    await matchmaker.remove_from_queue(message.from_user.id)
    await state.set_state(ChatStates.menu)
    await message.answer("🛑 Поиск отменен.", reply_markup=kb_main_menu())


@router.message(ChatStates.in_chat, F.text == "❌ Завершить чат")
async def end_chat(message: Message, state: FSMContext):
    partner_id = await matchmaker.close_chat(message.from_user.id)
    await state.set_state(ChatStates.menu)
    await message.answer("Вы завершили чат.", reply_markup=kb_main_menu())

    if partner_id:
        async with async_session() as session:
            await session.execute(update(Dialog).where(
                ((Dialog.user1_id == message.from_user.id) & (Dialog.user2_id == partner_id)) | (
                        (Dialog.user1_id == partner_id) & (Dialog.user2_id == message.from_user.id))).where(
                Dialog.is_active == True).values(is_active=False, end_time=datetime.now()))
            await session.commit()

        partner_storage_key = StorageKey(bot_id=message.bot.id, chat_id=partner_id, user_id=partner_id)
        partner_state = FSMContext(storage=state.storage, key=partner_storage_key)
        await partner_state.set_state(ChatStates.menu)
        try:
            await message.bot.send_message(partner_id, "⚠️ Собеседник завершил чат.", reply_markup=kb_main_menu())
        except Exception:
            pass


@router.message(ChatStates.in_chat, F.text == "🔄 Следующий собеседник")
async def next_chat(message: Message, state: FSMContext, db_user: Optional[User]):
    if not db_user: return
    partner_id = await matchmaker.close_chat(message.from_user.id)
    if partner_id:
        async with async_session() as session:
            await session.execute(update(Dialog).where(
                ((Dialog.user1_id == message.from_user.id) & (Dialog.user2_id == partner_id)) | (
                        (Dialog.user1_id == partner_id) & (Dialog.user2_id == message.from_user.id))).where(
                Dialog.is_active == True).values(is_active=False, end_time=datetime.now()))
            await session.commit()

        partner_storage_key = StorageKey(bot_id=message.bot.id, chat_id=partner_id, user_id=partner_id)
        partner_state = FSMContext(storage=state.storage, key=partner_storage_key)
        await partner_state.set_state(ChatStates.menu)
        try:
            await message.bot.send_message(partner_id, "⚠️ Собеседник покинул чат.", reply_markup=kb_main_menu())
        except Exception:
            pass

    await start_search_logic(message, state, db_user, search_gender=None, is_adult=False)


@router.message(ChatStates.in_chat, F.text == "🚩 Пожаловаться")
async def report_chat(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Спам", callback_data="rep_Спам"),
         InlineKeyboardButton(text="Оскорбления", callback_data="rep_Оскорбления")],
        [InlineKeyboardButton(text="Порно", callback_data="rep_Порно"),
         InlineKeyboardButton(text="Реклама", callback_data="rep_Реклама")],
        [InlineKeyboardButton(text="Другое", callback_data="rep_Другое")]
    ])
    await message.answer("Выберите причину жалобы на собеседника:", reply_markup=kb)


@router.callback_query(ChatStates.in_chat, F.data.startswith("rep_"))
async def process_report(callback: CallbackQuery):
    reason = callback.data.split("_")[1]
    partner_id = matchmaker.get_partner_id(callback.from_user.id)
    if not partner_id:
        await callback.answer("Собеседник уже вышел.")
        return

    async with async_session() as session:
        comp = Complaint(from_tg_id=callback.from_user.id, to_tg_id=partner_id, reason=reason)
        session.add(comp)
        await session.execute(
            update(User).where(User.tg_id == partner_id).values(complaint_count=User.complaint_count + 1))

        res = await session.execute(select(User.complaint_count).where(User.tg_id == partner_id))
        cnt = res.scalar() or 0
        if cnt >= 5:
            await session.execute(update(User).where(User.tg_id == partner_id).values(is_banned=True))
        await session.commit()

    await callback.message.answer(f"🛑 Жалоба принята ({reason}). Спасибо за бдительность!")
    await callback.answer()


# --- СИСТЕМА ПЕРЕСЫЛКИ СООБЩЕНИЙ ---
@router.message(ChatStates.in_chat)
async def messaging_bridge(message: Message, db_user: Optional[User]):
    partner_id = matchmaker.get_partner_id(message.from_user.id)
    if not partner_id:
        await message.answer("⚠️ Ошибка: Собеседник потерян. Вернитесь в главное меню.", reply_markup=kb_main_menu())
        return

    async with async_session() as session:
        res = await session.execute(
            select(User)
            .where(User.tg_id == partner_id)
            .options(joinedload(User.settings))
        )
        p_user = res.scalars().first()

    if not p_user:
        return

    s = p_user.settings
    partner_caption = f"💬 От собеседника: {message.caption}" if message.caption else "💬 От собеседника"

    try:
        if message.text:
            formatted_text = f"💬 **Собеседник:**\n{message.text}"
            await message.bot.send_message(partner_id, formatted_text, parse_mode="Markdown")
            return

        if message.photo:
            if s.allow_photo:
                await message.bot.send_photo(partner_id, message.photo[-1].file_id, caption=partner_caption)
            else:
                await message.answer("🔒 Собеседник отключил получение фото.")

        elif message.video:
            if s.allow_video:
                await message.bot.send_video(partner_id, message.video.file_id, caption=partner_caption)
            else:
                await message.answer("🔒 Собеседник отключил получение видео.")

        elif message.voice:
            if s.allow_voice:
                await message.bot.send_message(partner_id, "🎤 **Голосовое сообщение от собеседника:**",
                                               parse_mode="Markdown")
                await message.bot.send_voice(partner_id, message.voice.file_id)
            else:
                await message.answer("🔒 Собеседник отключил получение голосовых сообщений.")

        elif message.document:
            if s.allow_documents:
                await message.bot.send_document(partner_id, message.document.file_id, caption=partner_caption)
            else:
                await message.answer("🔒 Собеседник отключил получение документов.")

        elif message.sticker:
            await message.bot.send_message(partner_id, "🖼 **Стикер от собеседника:**", parse_mode="Markdown")
            await message.bot.send_sticker(partner_id, message.sticker.file_id)

        elif message.animation:
            await message.bot.send_animation(partner_id, message.animation.file_id, caption=partner_caption)

        elif message.video_note:
            await message.bot.send_message(partner_id, "📹 **Видеосообщение (кружок) от собеседника:**",
                                           parse_mode="Markdown")
            await message.bot.send_video_note(partner_id, message.video_note.file_id)

    except Exception as e:
        logging.error(f"Ошибка пересылки от {message.from_user.id} к {partner_id}: {e}")
        await message.answer("⚠️ Сообщение не доставлено. Возможно, собеседник заблокировал бота.")


# --- PREMIUM И КАТЕГОРИЯ ПОШЛЫЙ ЧАТ (18+) ---

# ИСПРАВЛЕНО: Добавлен хэндлер для обработки нажатия кнопки меню "Пошлый чат (18+)"
@router.message(F.text == "🔞 Пошлый чат (18+)")
async def cmd_adult_menu_button(message: Message, state: FSMContext, db_user: Optional[User]):
    if not db_user: return

    # Жесткая проверка на наличие премиума
    if not db_user.is_premium:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Купить Premium", callback_data="buy_premium_menu_redirect")]
        ])
        await message.answer(
            "🔒 **Этот режим доступен только Premium пользователям!**\n\n"
            "В пошлом чате вы сможете общаться без цензуры на любые темы. Оформите Premium, чтобы открыть доступ.",
            parse_mode="Markdown", reply_markup=kb
        )
        return

    # Проверка возраста по БД
    if db_user.age < 18:
        await message.answer("🚫 Вход воспрещен! Вам нет 18 лет согласно вашим регистрационным данным.")
        return

    # Если всё ок — отправляем инлайн-кнопку подтверждения входа
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Да, мне есть 18. Войти", callback_data="confirm_adult_search")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_adult")]
    ])
    await message.answer(
        "⚠️ Вы входите в категорию для взрослых. Продолжая, вы подтверждаете, что вам исполнилось 18 лет и вы берете на себя всю ответственность.",
        reply_markup=kb)


@router.callback_query(F.data == "buy_premium_menu_redirect")
async def process_premium_redirect(callback: CallbackQuery):
    await callback.message.delete()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏳ 1 час — 15 ⭐️", callback_data="buy_premium_1h")],
        [InlineKeyboardButton(text="⏰ 24 часа — 35 ⭐️", callback_data="buy_premium_24h")],
        [InlineKeyboardButton(text="📅 1 неделя — 75 ⭐️", callback_data="buy_premium_1w")],
        [InlineKeyboardButton(text="🗓 1 месяц — 115 ⭐️", callback_data="buy_premium_1m")],
        [InlineKeyboardButton(text="💎 Навсегда — 230 ⭐️", callback_data="buy_premium_forever")]
    ])
    purchase_info = ("💎 **Оформление Premium доступа**\n\n"
                     "Покупка Premium осуществляется вручную через Основателя.\n"
                     "Выберите подходящий тариф ниже, чтобы узнать детали оплаты:")
    await callback.message.answer(purchase_info, reply_markup=kb)
    await callback.answer()


@router.message(F.text == "⭐ Premium")
async def premium_status_menu(message: Message, db_user: Optional[User]):
    if not db_user: return

    if db_user.is_premium:
        if db_user.premium_until and db_user.premium_until.year > 2090:
            until_str = "навсегда"
        else:
            until_str = f"до {db_user.premium_until.strftime('%d.%m.%Y %H:%M')}"

        status_text = f"🟢 **АКТИВЕН** ({until_str})\n\nСпасибо за поддержку нашего проекта! Вам доступны все функции без ограничений."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔞 Войти в Пошлый чат (18+)", callback_data="enter_adult_zone")]
        ])
    else:
        status_text = "❌ **НЕ АКТИВЕН**\n\nВы пользуетесь базовой версией бота. Чтобы снять ограничения, выберите пункт «💎 Купить Premium» в меню."
        kb = None

    purchase_text = (f"⭐ **Ваш Premium статус:** {status_text}\n\n"
                     f"**Что дает Premium:**\n"
                     f"✅ Выбор пола собеседника при поиске\n"
                     f"✅ Приоритет при поиске (находит людей в 2 раза быстрее)\n"
                     f"✅ Доступ в закрытый режим '🔥 Пошлый чат (18+)'\n"
                     f"✅ Возможность отключать медиа (фото/видео/голос) в настройках\n"
                     f"✅ Премиальный значок в профиле")

    await message.answer(purchase_text, parse_mode="Markdown", reply_markup=kb)


@router.message(F.text == "💎 Купить Premium")
async def premium_purchase_menu(message: Message, db_user: Optional[User]):
    if not db_user: return

    if db_user.is_premium and db_user.premium_until and db_user.premium_until.year > 2090:
        await message.answer("✨ У вас уже активирован вечный Premium статус! Покупать повторно не нужно.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏳ 1 час — 15 ⭐️", callback_data="buy_premium_1h")],
        [InlineKeyboardButton(text="⏰ 24 часа — 35 ⭐️", callback_data="buy_premium_24h")],
        [InlineKeyboardButton(text="📅 1 неделя — 75 ⭐️", callback_data="buy_premium_1w")],
        [InlineKeyboardButton(text="🗓 1 месяц — 115 ⭐️", callback_data="buy_premium_1m")],
        [InlineKeyboardButton(text="💎 Навсегда — 230 ⭐️", callback_data="buy_premium_forever")]
    ])

    purchase_info = ("💎 **Оформление Premium доступа**\n\n"
                     "Покупка Premium осуществляется вручную через Основателя.\n"
                     "Выберите подходящий тариф ниже, чтобы узнать детали оплаты:")
    await message.answer(purchase_info, reply_markup=kb)


@router.callback_query(F.data == "enter_adult_zone")
async def adult_chat_entry(callback: CallbackQuery, state: FSMContext, db_user: Optional[User]):
    if not db_user: return
    if not db_user.is_premium:
        await callback.answer("🔒 Этот режим доступен только Premium пользователям!", show_alert=True)
        return
    if db_user.age < 18:
        await callback.answer("🚫 Вам нет 18 лет согласно регистрационным данным!", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Да, мне есть 18. Войти", callback_data="confirm_adult_search")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_adult")]
    ])
    await callback.message.answer(
        "⚠️ Вы входите в категорию для взрослых. Продолжая, вы подтверждаете, что вам исполнилось 18 лет и вы берете на себя всю ответственность.",
        reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("buy_premium_"))
async def process_buy_premium(callback: CallbackQuery):
    duration_type = callback.data.replace("buy_premium_", "")

    if duration_type == "1h":
        tariff = "1 час (15 ⭐️)"
    elif duration_type == "24h":
        tariff = "24 часа (35 ⭐️)"
    elif duration_type == "1w":
        tariff = "1 неделю (75 ⭐️)"
    elif duration_type == "1m":
        tariff = "1 месяц (115 ⭐️)"
    elif duration_type == "forever":
        tariff = "Навсегда (230 ⭐️)"
    else:
        await callback.answer()
        return

    text_instruction = (
        f"💳 **Покупка тарифа: {tariff}**\n\n"
        f"Для активации Premium доступа напишите нашему Основателю напрямую.\n"
        f"Перейдите по ссылке и отправьте свой Telegram ID: `{callback.from_user.id}`\n\n"
        f"👉 **Связаться с Основателем:** @solnvox\n\n"
        f"_После подтверждения оплаты звёздами или иным удобным способом, вам мгновенно начислят подписку!_"
    )

    await callback.message.answer(text_instruction, parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "confirm_adult_search")
async def process_adult_search(callback: CallbackQuery, state: FSMContext, db_user: Optional[User]):
    if not db_user: return
    await start_search_logic(callback.message, state, db_user, search_gender=None, is_adult=True)
    await callback.answer()


@router.callback_query(F.data == "cancel_adult")
async def cancel_adult(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


# --- ПАНЕЛЬ ОСНОВАТЕЛЯ И АДМИНИСТРАЦИЯ ---
@router.message(Command("founder"))
@router.message(Command("root"))
async def cmd_founder(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /founder ПАРОЛЬ")
        return
    if args[1] == FOUNDER_PASSWORD:
        async with async_session() as session:
            await session.execute(update(User).where(User.tg_id == message.from_user.id).values(role="founder"))
            await session.commit()
        await message.answer("👑 Права Основателя подтверждены! Вам доступны административные команды:\n\n"
                             "📊 `/stats` — Посмотреть статистику проекта\n"
                             "✉️ `/broadcast ТЕКСТ` — Сделать рассылку всем\n"
                             "🚫 `/ban ID` — Забанить пользователя\n"
                             "🔓 `/unban ID` — Разбанить пользователя\n"
                             "➕ `/giveprem ID ЧАСЫ` — Выдать Премиум (Укажите 0 для выдачи навсегда)\n"
                             "⭐ `/giveprem_all ДНИ` — Выдать Премиум ВСЕМ пользователям на указанное число дней (0 — навсегда)")
    else:
        await message.answer("❌ Неверный пароль.")


@router.message(Command("stats"))
async def admin_stats(message: Message, db_user: Optional[User]):
    if not db_user or db_user.role not in ["founder", "admin"]: return
    async with async_session() as session:
        day_ago = datetime.now() - timedelta(days=1)
        tot_u = await session.scalar(select(func.count(User.tg_id)))
        prem_u = await session.scalar(select(func.count(User.tg_id)).where(User.premium_until > datetime.now()))
        ban_u = await session.scalar(select(func.count(User.tg_id)).where(User.is_banned == True))
        tot_d = await session.scalar(select(func.count(Dialog.id)))
        tot_c = await session.scalar(select(func.count(Complaint.id)))
        reg_24 = await session.scalar(select(func.count(User.tg_id)).where(User.reg_date >= day_ago))

    in_q, in_c = matchmaker.get_stats()
    text = (f"📊 **Статистика бота:**\n\n"
            f" Всего пользователей: {tot_u}\n"
            f" Регистраций за сутки: {reg_24}\n"
            f" Premium пользователей: {prem_u}\n"
            f" Забанено: {ban_u}\n"
            f" Найдено пар за все время: {tot_d}\n"
            f" Подано жалоб: {tot_c}\n\n"
            f" 👥 В поиске прямо сейчас: {in_q}\n"
            f" 💬 Общаются прямо сейчас: {in_c} пар")
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("broadcast"))
async def admin_broadcast(message: Message, db_user: Optional[User]):
    if not db_user or db_user.role not in ["founder", "admin"]: return
    text_to_send = message.text.replace("/broadcast", "").strip()
    if not text_to_send:
        await message.answer("Пример: /broadcast Всем привет!")
        return

    async with async_session() as session:
        res = await session.execute(select(User.tg_id))
        ids = res.scalars().all()

    await message.answer(f"🚀 Запускаю рассылку на {len(ids)} пользователей...")
    success = 0
    for u_id in ids:
        try:
            await message.bot.send_message(u_id, f"📢 **Объявление от администрации:**\n\n{text_to_send}",
                                           parse_mode="Markdown")
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.answer(f"✅ Рассылка завершена. Успешно доставлено: {success}/{len(ids)}")


@router.message(Command("ban"))
async def admin_ban(message: Message, db_user: Optional[User]):
    if not db_user or db_user.role not in ["founder", "admin"]: return
    try:
        t_id = int(message.text.split()[1])
        async with async_session() as session:
            await session.execute(update(User).where(User.tg_id == t_id).values(is_banned=True))
            await session.commit()
        await message.answer(f"🚫 Пользователь {t_id} успешно заблокирован.")
    except Exception as e:
        await message.answer(f"Ошибка. Исполнение команды не удалось: {e}")


@router.message(Command("unban"))
async def admin_unban(message: Message, db_user: Optional[User]):
    if not db_user or db_user.role not in ["founder", "admin"]: return
    try:
        t_id = int(message.text.split()[1])
        async with async_session() as session:
            await session.execute(update(User).where(User.tg_id == t_id).values(is_banned=False))
            await session.commit()
        await message.answer(f"🔓 Пользователь {t_id} успешно разбанен.")
    except Exception as e:
        await message.answer(f"Ошибка при разбане: {e}")


@router.message(Command("giveprem"))
async def admin_giveprem(message: Message, db_user: Optional[User]):
    if not db_user or db_user.role not in ["founder", "admin"]: return
    try:
        args = message.text.split()
        if len(args) < 3:
            await message.answer("Использование: `/giveprem ID ЧАСЫ` (0 — навсегда)")
            return

        t_id = int(args[1].strip())
        hours = int(args[2])

        if hours == 0:
            new_until = datetime(2099, 12, 31, 23, 59, 59)
            msg_text = "навсегда"
        else:
            new_until = datetime.now() + timedelta(hours=hours)
            msg_text = f"на {hours} час(ов)"

        async with async_session() as session:
            await session.execute(update(User).where(User.tg_id == t_id).values(premium_until=new_until))
            await session.commit()
        await message.answer(f"⭐ Пользователю {t_id} успешно выдан Premium статус {msg_text}.")
        try:
            await message.bot.send_message(t_id, f"🎉 Администратор активировал вам Premium статус {msg_text}!")
        except Exception:
            pass
    except ValueError:
        await message.answer("❌ Ошибка: ID пользователя и ЧАСЫ должны быть исключительно числами.")
    except Exception as e:
        await message.answer(f"Ошибка при выдаче Premium: {e}")


@router.message(Command("giveprem_all"))
async def admin_giveprem_all(message: Message, db_user: Optional[User]):
    if not db_user or db_user.role not in ["founder", "admin"]: return
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer("Использование: `/giveprem_all ДНИ` (0 — навсегда)")
            return

        days = int(args[1])

        if days == 0:
            new_until = datetime(2099, 12, 31, 23, 59, 59)
            msg_text = "навсегда"
        else:
            new_until = datetime.now() + timedelta(days=days)
            msg_text = f"на {days} дней"

        async with async_session() as session:
            await session.execute(update(User).values(premium_until=new_until))
            await session.commit()

        await message.answer(f"⭐ Premium статус успешно выдан абсолютно ВСЕМ пользователям {msg_text}.")
    except ValueError:
        await message.answer("❌ Ошибка: Количество дней должно быть целым числом.")
    except Exception as e:
        await message.answer(f"Ошибка при массовой выдаче Premium: {e}")


# ==========================================
# 8. ЗАПУСК БОТА С АВТОСБРОСОМ ВЕБХУКА
# ==========================================
async def main():
    await init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    dp.message.outer_middleware(AntiFloodMiddleware())
    dp.callback_query.outer_middleware(AntiFloodMiddleware())
    dp.message.outer_middleware(AuthMiddleware())
    dp.callback_query.outer_middleware(AuthMiddleware())

    dp.include_router(router)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("Старый вебхук успешно удален. Запуск polling...")
    except Exception as e:
        logging.warning(f"Не удалось удалить вебхук: {e}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен.")

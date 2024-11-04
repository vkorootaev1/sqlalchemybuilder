from datetime import datetime
import logging
from typing import Annotated, AsyncIterator, List
from sqlalchemy import ARRAY, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, declared_attr, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.exc import SQLAlchemyError

from config import settings

DATABASE_URL = settings.get_db_url()

logger = logging.getLogger(__name__)


# Создаем асинхронный движок для работы с базой данных
engine = create_async_engine(url=DATABASE_URL, echo=True)
# Создаем фабрику сессий для взаимодействия с базой данных
async_session_maker = async_sessionmaker(engine, 
                                         autoflush=False, 
                                         expire_on_commit=False,
                                         )


async def get_session():
    async with async_session_maker() as session:
            yield session

# Базовый класс для всех моделей
class Base(AsyncAttrs, DeclarativeBase):
    __abstract__ = True  # Класс абстрактный, чтобы не создавать отдельную таблицу для него

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower() + 's'
import asyncio
from dataclasses import dataclass, field
from random import choice
from typing import Iterable, Self
from database import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, select
from models import *
from sqlalchemy.orm import InstrumentedAttribute, load_only, contains_eager
from sqlalchemy.orm.strategy_options import Load


@dataclass
class JoinPart:
    field_name_parts: list[str] = field(default_factory=list)
    



class SqlAlchemyQueryBuilder:
    
    @staticmethod
    def _get_join_parts(
        field_names: Iterable[str],
    ) -> list[JoinPart]:
        parts = []
        for field_name in field_names:
            field_name_parts = field_name.split(".")
            part = JoinPart()
            for field_name_part in field_name_parts:
                part.field_name_parts.append(field_name_part)
            parts.append(part)
            
        return parts
    
    @staticmethod
    def _get_field(
        model_class: Base,
        field_name: str,
    ) -> InstrumentedAttribute:
        field = getattr(model_class, field_name, None)
        if field is None:
            msg = f"'{model_class.__name__}' does not contain field '{field_name}'"
            raise ValueError (msg)
        return field
    
    @staticmethod
    def _get_relationship_field(
        model_class,
        relationship_name: str,
    ) -> InstrumentedAttribute:
        relationship_field = getattr(model_class, relationship_name, None)
        if relationship_field is None:
            msg = f"'{model_class.__name__}' does not contain relationship '{relationship_name}'"
            raise ValueError (msg)
        if not relationship_field.property._is_relationship:
            msg = f"'{relationship_name}' is not relationship of model '{model_class.__name__}'"
            raise ValueError (msg)
        return relationship_field
    
    def _set_join_to_stmt(
        self,
        model_class: Base,
        join_part: JoinPart,
    ) -> Select:
        contains_eager_chain = ()
        for field_name_part in join_part.field_name_parts:
            
            
                    
    def _add_joins_to_stmt(
        self,
        model_class: Base,
        join_fields_names: Iterable[str] | None = None,
    ) -> Select:
        parts = self._get_join_parts(join_fields_names)
        
    
    def build_query(
        self,
        model_class: Base,
        join_fields_names: Iterable[str] | None = None,
    ) -> Select:
        ...
        
        
async def test():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    # join_field_names = ("user", "user.profile")
    
    # stmt = SqlAlchemyQueryBuilder().build_query(
    #     Post,
    #     join_field_names,
    # )
    # print(stmt.compile())
    # result = await session.scalars(stmt)
    # result_orm = result.unique().all()
    # print('1')          
    
    stmt = (
        select(Post)
        .outerjoin(Post.user)
        .outerjoin(User.profile)
        .outerjoin(Post.comments)
        .options(contains_eager(Post.user).load_only(User.username))
        .options(contains_eager(Post.user, User.profile).load_only(Profile.age))
        .options(contains_eager(Post.comments).load_only(Comment.is_published))
    )
    
    print(stmt.compile())

asyncio.run(test())
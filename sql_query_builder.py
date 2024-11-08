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
class DFieldPart:
    name_parts: list[str] = field(default_factory=list)
    
@dataclass
class DJoinPart:
    relationship_name_parts: list[str] = field(default_factory=list)


class SqlAlchemyQueryBuilder:
    
    @staticmethod
    def _get_field_parts(
        field_names: list[str],
        separator: str = ".",
    ) -> list[DFieldPart]:
        field_parts = []
        for field_name in field_names:
            name_parts = field_name.split(separator)
            part = DFieldPart(name_parts)
            field_parts.append(part)
        return field_parts
    
    @staticmethod
    def _get_unique_join_parts(
        field_parts: list[DFieldPart], 
    ) -> list[DJoinPart]:
        unique_join_parts = []
        for field_part in field_parts:
            relationship_name_parts = field_part.name_parts[:-1]
            part = DJoinPart(relationship_name_parts)
            unique_join_parts.append(part)
        return list(unique_join_parts)
            
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
                        
    def _add_joins_to_stmt(
        self,
        stmt: Select,
        model_class: Base,
        field_parts: list[DFieldPart],
    ) -> Select:
        join_parts = self._get_unique_join_parts(field_parts)
        already_joined_models: list[type[Base]] = []
        for join_part in join_parts:
            model_class_ = model_class
            contains_eager_chain_values = []
            for relationship_name_part in join_part.relationship_name_parts:
                relationship_field = self._get_relationship_field(
                    model_class_,
                    relationship_name_part,
                )
                model_class_ = relationship_field.property.mapper.class_
                if model_class_ not in already_joined_models:
                    already_joined_models.append(relationship_name_part)
                    stmt = stmt.outerjoin(relationship_field)
                contains_eager_chain_values.append(relationship_field)
            stmt = stmt.options(contains_eager(*contains_eager_chain_values))
        
        return stmt

    def build_query(
        self,
        model_class: Base,
        field_names: list[str] | tuple[str] | set[str],
    ) -> Select:
        stmt = select(model_class)
        field_parts = self._get_field_parts(field_names)
        stmt = self._add_joins_to_stmt(
            stmt,
            model_class,
            field_parts,
        )
        return stmt
        
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
    
    # stmt = (
    #     select(Post)
    #     .outerjoin(Post.user)
    #     .outerjoin(User.profile)
    #     .outerjoin(Post.comments)
    #     .options(contains_eager(Post.user).load_only(User.username))
    #     .options(contains_eager(Post.user, User.profile).load_only(Profile.age))
    #     .options(contains_eager(Post.comments).load_only(Comment.is_published))
    # )
    
    # stmt = (
    #     select(Post)
    #     .outerjoin(Post.user)
    #     .outerjoin(User.profile)
    # )
    
    sql = SqlAlchemyQueryBuilder()
    stmt = sql.build_query(Post, ('id', 'name', 'user.username', 'user.profile.age'))
    
    print(stmt.compile())

asyncio.run(test())
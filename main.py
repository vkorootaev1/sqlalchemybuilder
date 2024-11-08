import asyncio
from dataclasses import dataclass, field
from random import choice
import random
from typing import Iterable, Self
from database import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, select
from models import *
from sqlalchemy.orm import InstrumentedAttribute, load_only, contains_eager


async def insert_profiles():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    profiles = [
        Profile(
            first_name=f"first_name_{i}",
            last_name=f"last_name_{i}",
            age=random.randint(10, 100)
            )
        for i in range (1, 6)
    ]
    session.add_all(profiles)
    await session.commit()


async def insert_users():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    users = [
        User(
            username=f"user_name_{i}",
            password=f"password_{i}",
            profile_id=i
            )
        for i in range (1, 6)
    ]
    session.add_all(users)
    await session.commit()

async def insert_posts():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    stmt = select(User.id)
    result = await session.execute(stmt)
    user_ids = [row.id for row in result.all()]
    
    posts = [
        Post(
            title=f"title {i}",
            content=f"content {i}",
            user_id=choice(user_ids)
        )
    for i in range(1, 1000)]
    
    session.add_all(posts)
    await session.commit()

async def insert_comments():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    stmt = select(User.id)
    result = await session.execute(stmt)
    user_ids = [row.id for row in result.all()]
    
    stmt = select(Post.id)
    result = await session.execute(stmt)
    posts_ids = [row.id for row in result.all()]
    
    comments = [
        Comment(
            content=f"content {i}",
            user_id=choice(user_ids),
            post_id=choice(posts_ids),
            is_published=choice([False, True]),
        )
    for i in range(1, 10000)]
    
    session.add_all(comments)
    await session.commit()
    
async def test():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    selectable_fields = (
        "user__username",
        "user__profile__first_name",
        "user__profile__last_name",
        "comments__is_published"
    )
    stmt = SqlAlchemyQueryBuilder().build_query(
        Post,
        ("user", "user__profile",),
        selectable_fields,
    )
    print(stmt.compile())
    result = await session.scalars(stmt)
    result_orm = result.unique().all()
    print('1')
    
@dataclass
class SelectableField:
    field_name: list[str]
    children: list[Self] = field(default_factory=list)
    
    def get_or_create_child(self, field_name: str):
        for child in self.children:
            if child.field_name == field_name:
                return child
        child = SelectableField(field_name=field_name)
        self.children.append(child)
        return child
    
    
    
class SqlAlchemyQueryBuilder:
    
    def __init__(self) -> None:
        self.stmt = None
        self.options = None
    
    @staticmethod
    def _get_selectable_fields(
        selectable_field_names: Iterable[str],
    ):
        selectable_fields = {}
        
        for field_name in selectable_field_names:
            field_name_parts = field_name.split('__')
            current_field_name_part = None
            for field_name_part in field_name_parts:
                if current_field_name_part is None:
                    if field_name_part not in selectable_fields:
                        current_field_name_part = SelectableField(field_name_part)
                        selectable_fields[field_name_part] = current_field_name_part
                    else:
                        current_field_name_part = selectable_fields[field_name_part]
                else:
                    current_field_name_part = current_field_name_part.get_or_create_child(field_name_part)
        
        return list(selectable_fields.values())
    
    def _get_relationship_model_class(
        self,
        model_class,
        relationship_name: str,
    ):
        relationship_field: InstrumentedAttribute | None = getattr(model_class, relationship_name, None)
        if relationship_field is None:
            raise ValueError ("qwe")
        if not relationship_field.property._is_relationship:
            raise ValueError ("qwe")
        return relationship_field.property.mapper.class_ 
    
    def _get_field(
        self,
        model_class: type[Base],
        field_name: str,
    ) -> InstrumentedAttribute:
        field = getattr(model_class, field_name, None)
        if field is None:
            raise ValueError ("qwe1232")
        return field
    
    def _set_options(
        self,
        relationship_name,
        model_class: type[Base],
        fields: list[InstrumentedAttribute],
    ):
        if self.options is None:
            self.options = contains_eager(getattr(model_class, relationship_name)).load_only(*fields)
        else:
            self.options = self.options.contains_eager(getattr(model_class, relationship_name)).load_only(*fields)
        
    
    def _set_selectable_fields(
        self,
        model_class: type[Base],
        field: SelectableField,
    ):
        children_fields = [record for record in field.children if not record.children]
        relationship_model_class = self._get_relationship_model_class(
                model_class,
                field.field_name,
            )
        if children_fields:
            fields = []
            for chidlren_field in children_fields:   
                fields.append(self._get_field(relationship_model_class, chidlren_field.field_name))
            self._set_options(field.field_name, model_class, fields)
        children_fields = [record for record in field.children if record.children]
        for chidlren_field in children_fields:
            self._set_selectable_fields(relationship_model_class, chidlren_field)
    
    def build_query(
        self,
        model_class: type[Base],
        join_field_names: Iterable[str],
        selectable_field_names: Iterable[str],
    ) -> Select:
        self.stmt = (select(Post)
                     .outerjoin(Post.user)
                     .outerjoin(Post.comments)
                     .outerjoin(User.profile))
        selectable_fields = self._get_selectable_fields(
            selectable_field_names,
        )
        for field in selectable_fields:
            self.options = None
            self._set_selectable_fields(model_class, field)
            self.stmt = self.stmt.options(self.options)
        return self.stmt
    
asyncio.run(insert_comments())
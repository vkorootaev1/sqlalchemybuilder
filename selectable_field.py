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
class BasePart:
    name: str
    children: list[Self] = field(default_factory=list)
    
    def get_or_create_child(self, name: str):
        for child in self.children:
            if child.name == name:
                return child
        child = SelectableFieldPart(name)
        self.children.append(child)
        return child


@dataclass
class SelectableFieldPart(BasePart):
    ...
    

@dataclass
class JoinPart(BasePart):
    ...
    

class SqlAlchemyBaseBuilder:
    
    def __init__(self) -> None:
        self._stmt: Select | None = None
        
class SqlAlchemyQueryBuilder(SqlAlchemyBaseBuilder):
        
    def __init__(self) -> None:
        self._stmt: Select | None = None
        self._contains_eager_chain: Load | None = None
        
    @staticmethod
    def _prepare_field_parts(
        field_names: Iterable[str],
        response_class: type[BasePart],
    ) -> list[type[BasePart]]:
        fields_parts = {}
        for field_name in field_names:
            field_name_parts = field_name.split('__')
            current_field_name_part = None
            for field_name_part in field_name_parts:
                if current_field_name_part is None:
                    if field_name_part not in fields_parts:
                        current_field_name_part = response_class(field_name_part)
                        fields_parts[field_name_part] = current_field_name_part
                    else:
                        current_field_name_part = fields_parts[field_name_part]
                else:
                    current_field_name_part = current_field_name_part.get_or_create_child(field_name_part)
        
        return list(fields_parts.values())
    
    @staticmethod
    def _get_field(
        model_class: type[Base],
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
        model_class: type[Base],
        join_field_part: JoinPart,
    ) -> None:
        field = self._get_field(model_class, join_field_part.name)
        self._stmt = self._stmt.outerjoin(field)
        nested_field_parts = [part for part in join_field_part.children]
        relationship_field = self._get_relationship_field(model_class, join_field_part.name)
        for nested_field_part in nested_field_parts:
            self._set_join_to_stmt(relationship_field.property.mapper.class_, nested_field_part)
    
    def set_joins_to_stmt(
        self,
        model_class: type[Base],
        join_field_names: Iterable[str],
    ) -> None:
        join_field_parts = self._prepare_field_parts(join_field_names, JoinPart)
        for join_field_part in join_field_parts:
            self._set_join_to_stmt(model_class, join_field_part)
        return self._stmt
    
    def _set_head_selectable_fields_to_stmt(
        self,
        model_class: type[Base],
        selectable_field_parts: list[SelectableFieldPart],
    ) -> None:
        head_selectable_field_parts = [
            selectable_field_part 
            for selectable_field_part in selectable_field_parts
            if not selectable_field_part.children
        ]
        fields = []
        for head_selectable_field_part in head_selectable_field_parts:
            field = self._get_field(model_class, head_selectable_field_part.name)
            fields.append(field)
        self._stmt = self._stmt.options(load_only(*fields))
        
    def _prepare_contains_eager_chain(
        self,
        model_class: type[Base],
        selectable_field_part: SelectableFieldPart,
    ) -> None:
        current_field_parts = [part for part in selectable_field_part.children if not part.children]
        relationship_field = self._get_relationship_field(model_class, selectable_field_part.name)
        if current_field_parts:
            fields = []
            for current_field_part in current_field_parts:
                field = self._get_field(relationship_field.property.mapper.class_, current_field_part.name)
                fields.append(field)
            if self._contains_eager_chain is None:
                self._contains_eager_chain = contains_eager(relationship_field).load_only(*fields)
            else:
                self._contains_eager_chain = self._contains_eager_chain.contains_eager(relationship_field).load_only(*fields)
        nested_field_parts = [part for part in selectable_field_part.children if part.children]
        for nested_field_part in nested_field_parts:
            self._prepare_contains_eager_chain(relationship_field.property.mapper.class_, nested_field_part)
                
    def _set_nested_selectable_fields_to_stmt(
        self,
        model_class,
        selectable_field_parts: list[SelectableFieldPart],
    ) -> None:
        nested_selectable_field_parts = [
            selectable_field_part 
            for selectable_field_part in selectable_field_parts
            if selectable_field_part.children
        ]
        for nested_selectable_field_part in nested_selectable_field_parts:
            self._contains_eager_chain = None
            self._prepare_contains_eager_chain(model_class, nested_selectable_field_part)
            self._stmt = self._stmt.options(self._contains_eager_chain)      
           
    def set_selectable_fields_to_stmt(
        self,
        model_class,
        selectable_field_names: Iterable[str] | None = None,
    ) -> None:
        selectable_field_parts = self._prepare_field_parts(selectable_field_names, SelectableFieldPart)
        self._set_head_selectable_fields_to_stmt(model_class, selectable_field_parts)
        self._set_nested_selectable_fields_to_stmt(model_class, selectable_field_parts)
    
    def build_query(
        self,
        model_class: type[Base],
        join_field_names: Iterable[str] | None = None,
        selectable_field_names: Iterable[str] | None = None,
    ) -> Select:
        self._stmt = select(model_class)
        self.set_joins_to_stmt(model_class, join_field_names)
        self.set_selectable_fields_to_stmt(model_class, selectable_field_names)
        return self._stmt
        
async def test():
    generator = get_session()
    session: AsyncSession = await generator.__anext__()
    
    join_field_names = ("user", "user__profile")
    selectable_field_names = ("title",)
    
    stmt = SqlAlchemyQueryBuilder().build_query(
        Post,
        join_field_names,
        selectable_field_names,
    )
    print(stmt.compile())
    result = await session.scalars(stmt)
    result_orm = result.unique().all()
    print('1')          

asyncio.run(test())
        
    
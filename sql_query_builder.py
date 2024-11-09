import asyncio
from dataclasses import dataclass, field
from random import choice
from typing import Any, Callable, Iterable, Self
from database import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, select
from models import *
from sqlalchemy.orm import InstrumentedAttribute, load_only, contains_eager
from sqlalchemy.orm.strategy_options import Load


@dataclass
class PreparedBaseField:
    name_parts: list[str]


@dataclass
class PreparedSelectableField(PreparedBaseField):
    ...


@dataclass
class PreparedJoinField(PreparedBaseField):
    ...


@dataclass
class PreparedFilter(PreparedBaseField):
    operator: str
    value: Any
    

class SqlAlchemyQueryBuilder:

    @staticmethod
    def _get_operators_mapping() -> dict[str, str]:
        return {
            "eq": "__eq__",
            "le": "__le__",
            "lt": "__lt__",
            "ne": "__ne__",
            "between": "between",
            "icontains": "icontains",
            "in": "in_",
            "ilike": "ilike",
            "not_in": "not_in",
        }

    @staticmethod
    def _get_field(
        model_class: type[Base],
        field_name: str,
    ) -> InstrumentedAttribute:
        field = getattr(model_class, field_name, None)
        if field is None:
            msg = f"'{model_class.__name__}' does not contain field '{field_name}'"
            raise ValueError(msg)
        return field

    @staticmethod
    def _get_relationship_field(
        model_class,
        relationship_name: str,
    ) -> InstrumentedAttribute:
        relationship_field = getattr(model_class, relationship_name, None)
        if relationship_field is None:
            msg = f"'{model_class.__name__}' does not contain relationship '{relationship_name}'"
            raise ValueError(msg)
        if not relationship_field.property._is_relationship:
            msg = f"'{relationship_name}' is not relationship of model '{model_class.__name__}'"
            raise ValueError(msg)
        return relationship_field

    @staticmethod
    def _generate_init_stmt(
        model_class: type[Base],
    ) -> Select:
        if issubclass(model_class, Base):
            return select(model_class)
        else:
            msg = f"'{model_class.__name__}' is not a sqlalchemy class"
            raise ValueError(msg)
        
    @staticmethod
    def _get_field_operator_function(
        field: InstrumentedAttribute,
        operator_name: str,
    ) -> Callable:
        operator_function = getattr(field, operator_name, None)
        if not operator_function:
            msg = f"{field} does not has operator function '{operator_name}'"
            raise ValueError (msg)
        return operator_function

    @staticmethod
    def _get_prepared_selectable_fields(
        field_names: list[str],
    ) -> list[PreparedSelectableField]:
        prepared_selectable_fields = []
        for field_name in field_names:
            name_parts = field_name.split(".")
            prepared_selectable_field = PreparedSelectableField(name_parts)
            prepared_selectable_fields.append(prepared_selectable_field)
        return prepared_selectable_fields

    @staticmethod
    def _get_prepared_join_fields(
        prepared_selectable_fields: list[PreparedSelectableField],
    ) -> list[PreparedJoinField]:
        unique_relationship_name_parts = []
        for prepared_selectable_field in prepared_selectable_fields:
            if len(prepared_selectable_field.name_parts) > 1:
                relationship_name_parts = prepared_selectable_field.name_parts[:-1]
                if relationship_name_parts not in unique_relationship_name_parts:
                    unique_relationship_name_parts.append(relationship_name_parts)
        return [
            PreparedJoinField(relationship_name_parts)
            for relationship_name_parts
            in unique_relationship_name_parts
        ]
        
    def _get_prepared_filters(
        self,
        filters: dict[str, Any]
    ) -> list[PreparedFilter]:
        possible_operators = self._get_operators_mapping().keys()
        prepared_filters = []
        for field_name, value in filters.items():
            name_parts = field_name.split(".")
            if name_parts[-1] in possible_operators:
                prepared_filter = PreparedFilter(
                    name_parts[:-1],
                    name_parts[-1],
                    value,
                )
            else:
                    prepared_filter = PreparedFilter(
                    name_parts,
                    "eq",
                    value,
                )
            prepared_filters.append(prepared_filter)
        return prepared_filters   

    @staticmethod
    def _clean_model_selectable_fields_mapping(
        model_selectable_fields_mapping: dict[type[Base], list[InstrumentedAttribute | str]],
    ) -> dict[type[Base], list[InstrumentedAttribute] | str]:
        for model, selectable_fields in model_selectable_fields_mapping.items():
            if "__all__" in selectable_fields:
                model_selectable_fields_mapping[model] = "__all__"
        return model_selectable_fields_mapping

    def _get_model_selectable_fields_mapping(
        self,
        model_class: type[Base],
        prepared_selectable_fields: list[PreparedSelectableField],
    ) -> dict[type[Base], list[InstrumentedAttribute] | str]:
        model_selectable_fields_mapping: dict[type[Base], list[InstrumentedAttribute | str]] = {}
        for prepared_selectable_field in prepared_selectable_fields:
            model_class_ = model_class
            for name_part in prepared_selectable_field.name_parts:
                if name_part == prepared_selectable_field.name_parts[-1]:
                    if name_part == "*":
                        field = "__all__"
                    else:
                        field = self._get_field(model_class_, name_part)
                else:
                    field = self._get_relationship_field(model_class_, name_part)
                    model_class_ = field.property.mapper.class_
            if model_class_ not in model_selectable_fields_mapping:
                model_selectable_fields_mapping[model_class_] = []
            model_selectable_fields_mapping[model_class_].append(field)
        model_selectable_fields_mapping = self._clean_model_selectable_fields_mapping(
            model_selectable_fields_mapping,
        )
        return model_selectable_fields_mapping

    @staticmethod
    def _set_head_selectable_fields_to_stmt(
        stmt: Select,
        model_class: type[Base],
        model_name_selectable_fields_mapping: dict[type[Base], list[InstrumentedAttribute] | str],
    ) -> Select:
        if model_name_selectable_fields_mapping.get(model_class, "__all__") == "__all__":
            return stmt
        else:
            selectable_fields = model_name_selectable_fields_mapping[model_class]
            return stmt.options(load_only(*selectable_fields))

    def _set_joins_and_nested_selectable_fields_to_stmt(
        self,
        stmt: Select,
        model_class: type[Base],
        prepared_join_fields: list[PreparedJoinField],
        model_name_selectable_fields_mapping: dict[type[Base], list[InstrumentedAttribute] | str],
    ) -> Select:
        already_joined_models: list[type[Base]] = []
        for prepared_join_field in prepared_join_fields:
            model_class_ = model_class
            contains_eager_chain_values = []
            for relationship_name in prepared_join_field.name_parts:
                relationship_field = self._get_relationship_field(
                    model_class_,
                    relationship_name,
                )
                model_class_ = relationship_field.property.mapper.class_
                if model_class_ not in already_joined_models:
                    already_joined_models.append(model_class_)
                    stmt = stmt.outerjoin(relationship_field)
                contains_eager_chain_values.append(relationship_field)
            if model_name_selectable_fields_mapping[model_class_] == "__all__":
                stmt = stmt.options(contains_eager(*contains_eager_chain_values))
            else:
                selectable_fields = model_name_selectable_fields_mapping[model_class_]
                stmt = stmt.options(contains_eager(*contains_eager_chain_values).load_only(*selectable_fields))
        return stmt

    def _set_filters_to_stmt(
        self,
        stmt: Select,
        model_class: type[Base],
        prepared_filters: list[PreparedFilter],
    ) -> Select:
        operators_mapping = self._get_operators_mapping()
        for prepared_filter in prepared_filters:
            model_class_ = model_class
            for name_part in prepared_filter.name_parts:
                if name_part == prepared_filter.name_parts[-1]:
                    field = self._get_field(model_class_, name_part)
                else:
                    field = self._get_relationship_field(model_class_, name_part)
                    model_class_ = field.property.mapper.class_
            operator_name = operators_mapping[prepared_filter.operator]
            field_operator_function = self._get_field_operator_function(
                field,
                operator_name,
            )
            stmt = stmt.where(field_operator_function(prepared_filter.value))
        return stmt

    def _add_joins_and_selectable_fields(
        self,
        stmt: Select,
        model_class: type[Base],
        field_names: list[str] | tuple[str] | set[str] | None = None,
    ) -> Select:
        if not field_names:
            return stmt
        
        prepared_selectable_fields = self._get_prepared_selectable_fields(field_names)
        model_name_selectable_fields_mapping = self._get_model_selectable_fields_mapping(
            model_class,
            prepared_selectable_fields,
        )
        stmt = self._set_head_selectable_fields_to_stmt(
            stmt,
            model_class,
            model_name_selectable_fields_mapping,
        )
        prepared_join_fields = self._get_prepared_join_fields(prepared_selectable_fields)
        stmt = self._set_joins_and_nested_selectable_fields_to_stmt(
            stmt,
            model_class,
            prepared_join_fields,
            model_name_selectable_fields_mapping,
        )
        return stmt

    def _add_filters(
        self,
        stmt: Select,
        model_class: type[Base],
        filters: dict[str, Any],
    ) -> Select:
        if not filters:
            return stmt

        prepared_filters = self._get_prepared_filters(filters)
        stmt = self._set_filters_to_stmt(
            stmt,
            model_class,
            prepared_filters,
        )
        return stmt

    def build_query(
        self,
        model_class: type[Base],
        fields: list[str] | tuple[str] | set[str] | None = None,
        filters: dict[str, Any] | None = None
    ) -> Select:
        stmt = self._generate_init_stmt(model_class)
        stmt = self._add_joins_and_selectable_fields(
            stmt,
            model_class,
            fields,
        )
        stmt = self._add_filters(
            stmt,
            model_class,
            filters,
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
    # filters = {'id': 1, 'id.in': [1, 2, 3], 'user.id': 5, 'user.id.in': [5, 6, 7]}
    # a = sql._get_prepared_filters(filters)
    # print('1')
    
    # stmt = sql.build_query(Post, ("*", "user.username", "user.password", "user.profile.age", "user.profile.last_name"))
    fields = ('title', 'user.username', 'user.profile.age', 'user.profile.last_name')
    filters = {'user.profile.first_name.icontains': "Ig"}
    stmt = sql.build_query(Post, fields, filters)

    # a = getattr(Profile.age, "in_")
    # filter = a([40, 50])

    # stmt = (
    #     select(Post)
    #     .outerjoin(Post.user)
    #     .outerjoin(User.profile)
    #     .outerjoin(Post.comments)
    #     .options(contains_eager(Post.user).load_only(User.username))
    #     .options(contains_eager(Post.user, User.profile).load_only(Profile.age))
    #     .options(contains_eager(Post.comments).load_only(Comment.is_published))
    #     .where(filter)
    # )

    print(stmt.compile(compile_kwargs={"literal_binds": True}))

    result = await session.scalars(stmt)
    result_orm = result.unique().all()
    print('1')

asyncio.run(test())

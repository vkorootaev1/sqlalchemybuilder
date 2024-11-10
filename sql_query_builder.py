import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Literal
from database import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, select
from models import *
from sqlalchemy.orm import InstrumentedAttribute, load_only, contains_eager


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
class PreparedFilterField(PreparedBaseField):
    operator: str
    value: Any


@dataclass
class PreparedOrderByField(PreparedBaseField):
    direction: Literal["asc", "desc"]


class SqlAlchemyQueryBuilder:
    """SqlAlchemy построитель запросов к базе данных."""

    @staticmethod
    def _get_operators_mapping() -> dict[str, str]:
        """Возвращает соответствие Sql-операторов.
        
        Возвращает соответствие между Sql-оператором, который необходимо передавать
            в построитель запросов и Sql-оператором синтаксиса SqlAlchemy.
            
        Extra:
            Можно добавить дополнительные Sql-операторы, которые представлены в SqlAlchemy.
        """
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
        """Возвращает поле SqlAlchemy модели по его наименованию. 
        
        Args:
            model_class (type[Base]): класс модели SqlAlchemy.
            field_name (str): наименование поля.
        
        Returns:
            InstrumentedAttribute: поле модели SqlAlchemy.
        
        Exceptions:
            ValueError: Если поле SqlAlchemy модели не найдено.
        """
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
        """Возвращает 'relationship' SqlAlchemy модели по его наименованию. 
        
        Args:
            model_class (type[Base]): класс модели SqlAlchemy.
            field_name (str): наименование 'relationship'.
        
        Returns:
            InstrumentedAttribute: поле 'relationship' модели SqlAlchemy.
        
        Exceptions:
            ValueError: Если поле 'relationship' SqlAclhemy модели не найдено
            ValueError: Если найденное поле не является 'relationship'
        """
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
        """Возвращает первоначальный запрос. 
        
        Инициализация первоначального запроса по переданной SqlAlchemy модели.
            
        Args:
            model_class (type[Base]): класс модели SqlAlchemy.
            
        Returns:
            Select: первоначальный запрос.
        """
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
        """Возвращает функцию Sql-оператора.
        
        Args:
            field (InstrumentedAttribute): поле модели SqlAlchemy.
            operator_name (str): наименование Sql-оператора в синтаксисе SqlAlchemy.
            
        Returns:
            operator_function (Callable): функция Sql-оператора.
        
        Exceptions:
            ValueError: Если функция Sql-оператора не найдена.
        """
        operator_function = getattr(field, operator_name, None)
        if not operator_function:
            msg = f"{field} does not has operator function '{operator_name}'"
            raise ValueError (msg)
        return operator_function
    
    @staticmethod
    def _get_order_by_field_direction(
        order_by_field: str,
    ) -> Literal["asc", "desc"]:
        """Возвращает направление сортировки у поля.
        
        Args:
            order_by_field (str): поле сортировки в строковом представлении.
            
        Returns:
            Literal["asc", "desc"]: направление сортировки (возр, убыв.).
        """
        if order_by_field.startswith("-"):
            return "desc"
        else:
            return "asc"
        
    @staticmethod
    def _remove_order_by_field_direction_prefix(
        order_by_field: str,
    ) -> str:
        """Удаляет префикс направления сортировки из поля.
        
        Args:
            order_by_field (str): поле сортировки в строковом представлении.
        
        Returns:
            str: поле сортировки в строковом представлении без префикса направления сортировки.
        """
        if order_by_field.startswith("-"):
            return order_by_field.removeprefix("-")
        else:
            return order_by_field

    @staticmethod
    def _get_prepared_selectable_fields(
        field_names: list[str] | tuple[str] | set[str],
    ) -> list[PreparedSelectableField]:
        """Возвращает подготовленные выбираемые поля.
        
        Args:
            field_names (list[str] | tuple[str] | set[str]): список выбираемых полей в строковом представлении.
        
        Returns:
            list[PreparedSelectableField]: список подготовленных выбираемых полей.
        """
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
        """Возвращает подготовленные поля для соединения между таблицами (моделями).
        
        Args:
            prepared_selectable_fields (list[PreparedSelectableField]): список подготовленных выбираемых полей.
        
        Returns:
            list[PreparedJoinField]: список подготовленных полей для соединения между таблицами (моделями).
        """
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
    ) -> list[PreparedFilterField]:
        """Возвращает подготовленные части фильтрации.
        
        Args:
            filters (dict[str, Any]): словарь, хранящий части фильтрации.
                Ключом словаря является наименование поле с sql-оператором в строком представлении.
                Значением словаря является значение, по которому нужно фильтровать.
        
        Returns:
            list[PreparedFilterField] - список подготовленных частей фильтрации.
        """
        possible_operators = self._get_operators_mapping().keys()
        prepared_filters = []
        for field_name, value in filters.items():
            name_parts = field_name.split(".")
            if name_parts[-1] in possible_operators:
                prepared_filter = PreparedFilterField(
                    name_parts[:-1],
                    name_parts[-1],
                    value,
                )
            else:
                    prepared_filter = PreparedFilterField(
                    name_parts,
                    "eq",
                    value,
                )
            prepared_filters.append(prepared_filter)
        return prepared_filters   
    
    def _get_prepared_order_by_fields(
        self,
        order_by_fields: list[str] | tuple[str] | set[str],
    ) -> list[PreparedOrderByField]:
        """Возвращает подготовленные поля сортировки.
        
        Args:
            order_by_fields (list[str] | tuple[str] | set[str]): список полей сортировки в строковом представлении.
        
        Returns:
            list[PreparedSelectableField]: список подготовленных полей сортировки.
        """
        prepared_order_by_fields = []
        for order_by_field in order_by_fields:
            direction = self._get_order_by_field_direction(order_by_field)
            order_by_field = self._remove_order_by_field_direction_prefix(order_by_field)
            name_parts = order_by_field.split(".")
            prepared_order_by_field = PreparedOrderByField(
                name_parts,
                direction,
            )
            prepared_order_by_fields.append(prepared_order_by_field)
        return prepared_order_by_fields

    @staticmethod
    def _clean_model_selectable_fields_mapping(
        model_selectable_fields_mapping: dict[type[Base], list[InstrumentedAttribute | str]],
    ) -> dict[type[Base], list[InstrumentedAttribute] | str]:
        """Возвращает очищенные соответствие между моделями SqlAlchemy и выбираемыми полями.
        
        Если для модели выбраны и все поля, и некоторые поля, то будут выбраны все поля.
        
        Args:
            model_selectable_fields_mapping: dict[type[Base], list[InstrumentedAttribute | str]]: 
                соответствие между моделями SqlAlchemy моделями и выбираемыми полями.
        
        Returns:
            dict[type[Base], list[InstrumentedAttribute] | str]: 
                очищенные соответствие между моделями SqlAlchemy и выбираемыми полями
        """
        for model, selectable_fields in model_selectable_fields_mapping.items():
            if "__all__" in selectable_fields:
                model_selectable_fields_mapping[model] = "__all__"
        return model_selectable_fields_mapping

    def _get_model_selectable_fields_mapping(
        self,
        model_class: type[Base],
        prepared_selectable_fields: list[PreparedSelectableField],
    ) -> dict[type[Base], list[InstrumentedAttribute] | str]:
        """Возвращает соответствие между моделями SqlAlchemy и выбираемыми полями.
        
        Args:
            model_class (type[Base]): класс SqlAlchemy модели.
            prepared_selectable_fields (list[PreparedSelectableField]): список подготовленных выбираемых полей
            
        Returns:
            dict[type[Base], list[InstrumentedAttribute] | str]: 
                словарь, хранящий соответствие между моделями SqlAlchemy и выбираемыми полями.
                Ключом словаря является класс SqlAlchemy модели.
                Значением словаря является список выбираемых для модели полей, 
                    либо строка, где указано, что выбираются все поля модели ("__all__").
        """
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
        """Устанавливает в запрос выбираемые поля для главной SqlAlchemy модели.
        
        Description:
            Если найдены выбираемые поля для главной SqlAclhemy модели, то устанавливаются они.
            Иначе будут выбраны все поля.
            
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            model_name_selectable_fields_mapping (dict[type[Base], list[InstrumentedAttribute] | str]):
                соответствие между моделями SqlAlchemy и выбираемыми полями.
        
        Returns:
            Select: обновленный запрос.
        """
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
        """Устанавливает в запрос соединения между таблицами (моделями) и выбираемые поля присоединенных таблиц.
        
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            prepared_join_fields (list[PreparedJoinField]): 
                список подготовленных полей для соединения между таблицами (моделями).
            model_name_selectable_fields_mapping (dict[type[Base], list[InstrumentedAttribute] | str]):
                соответствие между моделями SqlAlchemy и выбираемыми полями.
        
        Returns:
            Select: обновленный запрос.
        """
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
        prepared_filters: list[PreparedFilterField],
    ) -> Select:
        """Устанавливает в запрос фильтрацию.
            
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            prepared_filters (list[PreparedFilterField]): список подготовленных частей фильтрации.
        
        Returns:
            Select: обновленный запрос.
        """
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
    
    def _set_order_by_to_stmt(
        self,
        stmt: Select,
        model_class: type[Base],
        prepared_order_by_fields: list[PreparedOrderByField],
    ) -> Select:
        """Устанавливает в запрос сортировку.
            
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            prepared_order_by_fields (list[PreparedOrderByField]): список подготовленных полей сортировки.
        
        Returns:
            Select: обновленный запрос.
        """
        for prepared_order_by_field in prepared_order_by_fields:
            model_class_ = model_class
            for name_part in prepared_order_by_field.name_parts:
                if name_part == prepared_order_by_field.name_parts[-1]:
                    field = self._get_field(model_class_, name_part)
                else:
                    field = self._get_relationship_field(model_class_, name_part)
                    model_class_ = field.property.mapper.class_
            if prepared_order_by_field.direction == "asc":
                stmt = stmt.order_by(field)
            else:
                stmt = stmt.order_by(field.desc())
        return stmt
                
    def _add_joins_and_selectable_fields(
        self,
        stmt: Select,
        model_class: type[Base],
        field_names: list[str] | tuple[str] | set[str] | None,
    ) -> Select:
        """Добавление в запрос соединения между таблицами (моделями) и выбираемые поля присоединенных таблиц.
            
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            field_names (list[str] | tuple[str] | set[str] | None): список выбираемых полей в строковом представлении.
        
        Returns:
            Select: обновленный запрос.
        """
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
        filters: dict[str, Any] | None,
    ) -> Select:
        """Добавление в запрос фильтрации.
            
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            filters (dict[str, Any] | None): словарь, хранящий части фильтрации.
                Ключом словаря является наименование поле с sql-оператором в строком представлении.
                Значением словаря является значение, по которому нужно фильтровать.
        
        Returns:
            Select: обновленный запрос.
        """
        if not filters:
            return stmt

        prepared_filters = self._get_prepared_filters(filters)
        stmt = self._set_filters_to_stmt(
            stmt,
            model_class,
            prepared_filters,
        )
        return stmt
    
    def _add_order_by(
        self,
        stmt: Select,
        model_class: type[Base],
        order_by: list[str] | tuple[str] | set[str] | None,
    ) -> Select:
        """Добавление в запрос сортировки.
            
        Args:
            stmt (Select): запрос.
            model_class (type[Base]): класс модели SqlAlchemy.
            order_by (list[str] | tuple[str] | set[str] | None): список полей сортировки в строковом представлении.

        Returns:
            Select: обновленный запрос.
        """
        if not order_by:
            return stmt
        
        prepared_order_by_fields = self._get_prepared_order_by_fields(
            order_by,
        )
        stmt = self._set_order_by_to_stmt(
            stmt,
            model_class,
            prepared_order_by_fields,
        )
        return stmt
    
    @staticmethod
    def _add_limit_to_stmt(
        stmt: Select,
        limit: int | None
    ) -> Select:
        """Добавление в запрос ограничения количества выгружаемых строк.
            
        Args:
            stmt (Select): запрос.
            limit (int | None): количество выгружаемых строк.

        Returns:
            Select: обновленный запрос.
        """
        if not limit:
            return stmt
        
        stmt = stmt.limit(limit)
        return stmt
    
    @staticmethod
    def _add_offset_to_stmt(
        stmt: Select,
        offset: int | None
    ) -> Select:
        """Добавление в запрос смещения.
            
        Args:
            stmt (Select): запрос.
            offset (int | None): показатель смещения.

        Returns:
            Select: обновленный запрос.
        """
        if not offset:
            return stmt
        
        stmt = stmt.offset(offset)
        return stmt
    
    def build_query(
        self,
        model_class: type[Base],
        fields: list[str] | tuple[str] | set[str] | None = None,
        filters: dict[str, Any] | None = None,
        order_by: list[str] | tuple[str] | set[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Select:
        """Возвращает построенный запрос по переданным параметрам.
        
        Документация по использованию построителя запроса размещена в репозитории 'backend/fastapi' 
            в файле 'sql_builder_doc.md'.
        
        Args:
            model_class (type[Base]): 
                модель SqlAlchemy, от который будет осуществляться построение запроса.
            fields (list[str] | tuple[str] | set[str] | None = None): 
                список выбираемых полей в строковом представлении.
            filters (dict[str, Any] | None = None): словарь, хранящий части фильтрации.
                Ключом словаря является наименование поле с sql-оператором в строком представлении.
                Значением словаря является значение, по которому нужно фильтровать.
            order_by (list[str] | tuple[str] | set[str] | None = None): 
                список полей сортировки в строковом представлении.
            limit (int | None = None): 
                ограничение количества выгружаемых строк.
            offset (int | None = None): 
                показтель смещения.
                
        Returns:
            Select: построенный запрос.
        """
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
        stmt = self._add_order_by(
            stmt,
            model_class,
            order_by,
        )
        stmt = self._add_limit_to_stmt(stmt, limit)
        stmt = self._add_offset_to_stmt(stmt, offset)
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
    filters = {'user.profile.age.in': [20, 40]}
    order_by = ('user.profile.age', "-user.profile.first_name")
    limit = 300
    offset = 186
    stmt = sql.build_query(Post, fields, filters, order_by=order_by, limit=limit, offset=offset)

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

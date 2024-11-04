from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database import Base


class User(Base):
    username: Mapped[str]
    password: Mapped[str]
    profile_id: Mapped[int | None] = mapped_column(ForeignKey('profiles.id'))

    profile: Mapped["Profile"] = relationship(
        "Profile",
        back_populates="user",
        uselist=False,
    )

    # Связь один-ко-многим с Post
    posts: Mapped[list["Post"]] = relationship(
        "Post",
        back_populates="user",
        cascade="all, delete-orphan"  # При удалении User удаляются и связанные Post
    )

    # Связь один-ко-многим с Comment
    comments: Mapped[list["Comment"]] = relationship(
        "Comment",
        back_populates="user",
        cascade="all, delete-orphan"  # При удалении User удаляются и связанные Comment
    )
    
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.id=} {self.username=} {self.email=} {self.password=})'


class Profile(Base):
    first_name: Mapped[str]
    last_name: Mapped[str]
    age: Mapped[int]
    
    user: Mapped["User"] = relationship(
        "User",
        back_populates="profile",
        uselist=False
    )


class Post(Base):
    title: Mapped[str]
    content: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))

    user: Mapped["User"] = relationship(
        "User",
        back_populates="posts"
    )

    comments: Mapped[list["Comment"]] = relationship(
        "Comment",
        back_populates="post",
        cascade="all, delete-orphan"
    )


class Comment(Base):
    content: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
    post_id: Mapped[int] = mapped_column(ForeignKey('posts.id'))
    is_published: Mapped[bool] = mapped_column(default=True)

    user: Mapped["User"] = relationship(
        "User",
        back_populates="comments"
    )

    post: Mapped["Post"] = relationship(
        "Post",
        back_populates="comments"
    )
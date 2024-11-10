from models import Post

a = Post.id
b = getattr(a, "not_in", None)
print(b)
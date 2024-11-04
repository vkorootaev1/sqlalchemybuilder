from dataclasses import dataclass, field
from typing import Self


from dataclasses import dataclass, field
from typing import List

@dataclass
class TreeNode:
    name: str
    children: List['TreeNode'] = field(default_factory=list)

    def add_child(self, child_name: str):
        # Проверяем, существует ли уже дочерний узел с таким именем
        for child in self.children:
            if child.name == child_name:
                return child  # Возвращаем существующий узел
        # Если нет, создаем нового ребенка и добавляем в список
        new_child = TreeNode(name=child_name)
        self.children.append(new_child)
        return new_child

def build_trees(flat_list):
    trees = {}  # Словарь для хранения корневых узлов

    for path in flat_list:
        parts = path.split("__")
        current_node = None  # Указатель на текущий узел

        for part in parts:
            if current_node is None:
                # Создаем новый корневой узел, если текущий узел не задан
                if part not in trees:
                    current_node = TreeNode(name=part)
                    trees[part] = current_node  # Добавляем корень в словарь
                else:
                    current_node = trees[part]  # Используем существующий корень
            else:
                # Ищем или создаем новый узел для следующего уровня
                current_node = current_node.add_child(part)

    return list(trees.values())  # Возвращаем список корневых узлов

# Пример использования
flat_list = ["user__name", "user__id", "user__profile__lastname", "admin__id", "admin__profile__email"]
trees = build_trees(flat_list)

print('1')
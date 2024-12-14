from rectangle import rectangle
from  triangle import triangle

rect = rectangle()
tri = triangle()

rect.set_values(20,34)
tri.set_values(10,20)
rect.set_color('red')
tri.set_color('blue')
print(rect.area())
print(tri.area())
print(rect.get_color())
print(tri.get_color())
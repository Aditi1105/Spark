from polygon import polygon
from shape import shape
class rectangle (polygon,shape):
    def area(self):
        return self.get_width() * self.get_height()
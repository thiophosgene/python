import math

def circle_area(radius):   
    if radius <= 0:
        raise ValueError("Радиус должен быть неотрицательным")
    return math.pi * radius ** 2

def triangle_area(a, b, c):
    if a <= 0 or b <= 0 or c <= 0:
        raise ValueError("Длина сторон должна быть положительной")
    
    if (a + b <= c) or (a + c <= b) or (b + c <= a):
        raise ValueError("Не существует треугольника с данными длинами сторон")
    
    p = (a + b + c) / 2
    area = math.sqrt(p * (p - a) * (p - b) * (p - c))
    return area

def is_rectangular_triangle(a, b, c, tolerance=1e-6):
    
    if a <= 0 or b <= 0 or c <= 0:
        raise ValueError("Все длины сторон должны быть положительными")
    
    if (a + b <= c) or (a + c <= b) or (b + c <= a):
        raise ValueError("Не существует треугольника с данными длинами сторон")
    
    sides = sorted([a, b, c])
    a, b, c = sides
    
    # Check Pythagorean theorem with some tolerance for floating point precision
    return abs(a**2 + b**2 - c**2) < tolerance

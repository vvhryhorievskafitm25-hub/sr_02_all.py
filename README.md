# sr_02_all.py
"""
Самостійна робота №2: Об'єктно-орієнтована система управління базою даних книг у бібліотеці
Виконано студенткою групи КБ-25: Григор'євська Вікторія
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Any


# =====================================================================
# 1. ДОМЕННІ ВИКЛЮЧЕННЯ
# =====================================================================
class LibraryError(Exception):
    """Базовий клас для всіх помилок бібліотечної системи."""
    def __init__(self, message: str, error_code: int = 4000):
        super().__init__(message)
        self.error_code = error_code

    def __str__(self) -> str:
        return f"[LibraryError Код {self.error_code}]: {super().__str__()}"


class BookNotFoundError(LibraryError):
    """Виникає, коли книгу не знайдено в базі даних."""
    def __init__(self, isbn: str):
        super().__init__(f"Книгу з ISBN '{isbn}' не знайдено.", 4001)
        self.isbn = isbn


class ValidationError(LibraryError):
    """Виникає при порушенні правил валідації атрибутів сутностей."""
    def __init__(self, message: str, field_name: str):
        super().__init__(message, 4002)
        self.field_name = field_name


# =====================================================================
# 2. МЕТАКЛАСИ, ДЕСКРИПТОРИ ТА МОДЕЛІ
# =====================================================================
class LoggerMeta(type):
    """Метаклас для автоматичного логування створення класів та їх екземплярів."""
    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        print(f"[METACLASS LOG] Створено новий екземпляр класу '{cls.__name__}'")
        return instance


class ValidatedField:
    """Дескриптор для перевірки типів та діапазонів значень атрибутів."""
    def __init__(self, expected_type: type, min_val=None, max_val=None):
        self.expected_type = expected_type
        self.min_val = min_val
        self.max_val = max_val
        self.name = None
        self.private_name = None

    def __set_name__(self, owner, name):
        self.name = name
        self.private_name = f"_{name}"

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return getattr(obj, self.private_name, None)

    def __set__(self, obj, value):
        if not isinstance(value, self.expected_type):
            raise ValidationError(
                f"Поле '{self.name}' має бути типу {self.expected_type.__name__}, а отримано {type(value).__name__}",
                self.name
            )
        if self.min_val is not None and value < self.min_val:
            raise ValidationError(f"Поле '{self.name}' не може бути меншим за {self.min_val}", self.name)
        if self.max_val is not None and value > self.max_val:
            raise ValidationError(f"Поле '{self.name}' не може перевищувати {self.max_val}", self.name)
        setattr(obj, self.private_name, value)


@dataclass
class Author(metaclass=LoggerMeta):
    name: str
    birth_year: int = ValidatedField(int, min_val=1500, max_val=2026)
    country: str = "Україна"


@dataclass
class Book(metaclass=LoggerMeta):
    title: str
    author: str
    isbn: str
    year: int = ValidatedField(int, min_val=1500, max_val=2026)
    copies: int = ValidatedField(int, min_val=0, max_val=1000)
    available: bool = True


@dataclass
class Reader(metaclass=LoggerMeta):
    reader_id: str
    full_name: str
    email: str
    borrowed_books: list = field(default_factory=list)


# =====================================================================
# 3. БАЗА ДАНИХ (SINGLETON) ТА СПОСТЕРІГАЧ (OBSERVER)
# =====================================================================
class SingletonMeta(type):
    """Метаклас для реалізації паттерну Singleton."""
    _instances: dict[type, object] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class EventEmitter:
    """Базовий клас для реалізації паттерну Observer (система подій)."""
    def __init__(self):
        self._listeners: Dict[str, List[Callable]] = {}

    def on(self, event: str, callback: Callable):
        self._listeners.setdefault(event, []).append(callback)

    def emit(self, event: str, *args, **kwargs):
        for callback in self._listeners.get(event, []):
            callback(*args, **kwargs)


class LibraryDatabase(EventEmitter, metaclass=SingletonMeta):
    """Репозиторій книг та читачів бібліотеки (Singleton)."""
    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        super().__init__()
        self.books: Dict[str, Book] = {}
        self.readers: Dict[str, Reader] = {}
        self._initialized = True

    def add_book(self, book: Book):
        self.books[book.isbn] = book
        self.emit("book_added", book)

    def get_book(self, isbn: str) -> Book:
        if isbn not in self.books:
            raise BookNotFoundError(isbn)
        return self.books[isbn]

    def register_reader(self, reader: Reader):
        self.readers[reader.reader_id] = reader
        self.emit("reader_registered", reader)


# =====================================================================
# 4. ПАТТЕРНИ ПРОЕКТУВАННЯ: STRATEGY ТА FACTORY METHOD
# =====================================================================
class SortStrategy(ABC):
    @abstractmethod
    def sort(self, books: list[Book]) -> list[Book]:
        pass


class SortByTitle(SortStrategy):
    def sort(self, books: list[Book]) -> list[Book]:
        return sorted(books, key=lambda b: b.title)


class SortByYear(SortStrategy):
    def sort(self, books: list[Book]) -> list[Book]:
        return sorted(books, key=lambda b: b.year, reverse=True)


class EntityFactory(ABC):
    @abstractmethod
    def create_entity(self, *args, **kwargs):
        pass


class BookFactory(EntityFactory):
    def create_entity(self, title: str, author: str, isbn: str, year: int, copies: int) -> Book:
        return Book(title=title, author=author, isbn=isbn, year=year, copies=copies)


class ReaderFactory(EntityFactory):
    def create_entity(self, reader_id: str, full_name: str, email: str) -> Reader:
        return Reader(reader_id=reader_id, full_name=full_name, email=email)


# =====================================================================
# 5. МУЛЬТИПОТОЧНИЙ СЕРВІС ЗАПИТІВ
# =====================================================================
class LibraryConcurrencyManager:
    """Менеджер для виконання паралельних запитів до бази даних через ThreadPoolExecutor."""
    def __init__(self, db: LibraryDatabase, max_workers: int = 4):
        self.db = db
        self.max_workers = max_workers
        self._lock = threading.Lock()

    def process_parallel_queries(self, isbn_list: list[str]) -> dict[str, str]:
        """Паралельно виконує пошук книг у базі даних із захистом через Lock."""
        results = {}

        def query_task(isbn: str):
            with self._lock:
                try:
                    book = self.db.get_book(isbn)
                    return isbn, f"Знайдено: '{book.title}' ({book.author})"
                except Exception as e:
                    return isbn, f"Помилка: {e}"

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_isbn = {executor.submit(query_task, isbn): isbn for isbn in isbn_list}
            for future in as_completed(future_to_isbn):
                isbn, res = future.result()
                results[isbn] = res

        return results


# =====================================================================
# ГОЛОВНИЙ МОДУЛЬ ВИКОНАННЯ
# =====================================================================
def main():
    print("==========================================================")
    print(" ОБ'ЄКТНО-ОРІЄНТОВАНА СИСТЕМА УПРАВЛІННЯ БАЗОЮ ДАНИХ (СР №2)")
    print(" Студентка групи КБ-25: Григор'євська Вікторія")
    print("==========================================================")

    # 1. Ініціалізація бази даних (Singleton) та Observer
    db = LibraryDatabase()
    db.on("book_added", lambda b: print(f"[OBSERVER] Додано книгу: '{b.title}'"))
    db.on("reader_registered", lambda r: print(f"[OBSERVER] Зареєстровано читача: {r.full_name}"))

    # 2. Фабричний метод для створення сутностей
    print("\n--- [1] Створення сутностей (Factory Method) ---")
    book_factory = BookFactory()
    reader_factory = ReaderFactory()

    b1 = book_factory.create_entity("Чистий код", "Роберт Мартін", "978-5-8459-1786-7", 2018, 5)
    b2 = book_factory.create_entity("Програміст-практик", "Ендрю Гант", "978-5-8459-0959-6", 2015, 3)
    b3 = book_factory.create_entity("Штучний інтелект", "Сюю Еллен", "978-1-119-60000-1", 2022, 4)

    db.add_book(b1)
    db.add_book(b2)
    db.add_book(b3)

    reader1 = reader_factory.create_entity("R001", "Олександр Петренко", "petrenko@gmail.com")
    db.register_reader(reader1)

    # 3. Демонстрація валідації через дескриптори та обробки виключень
    print("\n--- [2] Демонстрація валідації та виключень ---")
    try:
        book_factory.create_entity("Некоректна книга", "Автор", "123", 1300, 10)
    except ValidationError as e:
        print(f"Перехоплено очікувану помилку валідації: {e}")

    # 4. Демонстрація патерну Strategy для сортування
    print("\n--- [3] Сортування каталогу (Strategy) ---")
    sorter = SortByTitle()
    sorted_books = sorter.sort(list(db.books.values()))
    for book in sorted_books:
        print(f"  * {book.title} ({book.year}) — {book.author}")

    # 5. Демонстрація мультипоточності (ThreadPoolExecutor + Lock)
    print("\n--- [4] Паралельні запити до бази даних (Мультипоточність) ---")
    concurrency_manager = LibraryConcurrencyManager(db, max_workers=3)
    search_isbns = [
        "978-5-8459-1786-7",
        "978-5-8459-0959-6",
        "978-0-0000-0000-0",  # Неіснуючий ISBN
        "978-1-119-60000-1"
    ]
    
    results = concurrency_manager.process_parallel_queries(search_isbns)
    for isbn, res in results.items():
        print(f"  Запит [ISBN: {isbn}] -> {res}")

    print("\nРоботу програми успішно завершено.")


if __name__ == "__main__":
    main()

from pathlib import Path

Path(__file__).with_name("imported.txt").write_text("imported", encoding="utf-8")


class GreeterPlugin:
    pass

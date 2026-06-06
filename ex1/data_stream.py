import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._buffer: list[typing.Any] = []
        self._total_processed: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> typing.Any:
        if self._buffer:
            return self._buffer.pop(0)
        return None

    @property
    def total_processed(self) -> int:
        return self._total_processed

    @property
    def remaining(self) -> int:
        return len(self._buffer)

    @abc.abstractmethod
    def name(self) -> str:
        pass


class NumericProcessor(DataProcessor):
    def name(self) -> str:
        return "Numeric Processor"

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(item, (int, float))
                       and not isinstance(item, bool)
                       for item in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        values = [data] if isinstance(data, (int, float)) else data
        for value in values:
            self._buffer.append(value)
            self._total_processed += 1


class TextProcessor(DataProcessor):
    def name(self) -> str:
        return "Text Processor"

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        texts = [data] if isinstance(data, str) else data
        for text in texts:
            self._buffer.append(text)
            self._total_processed += 1


class LogProcessor(DataProcessor):
    _REQUIRED_KEYS = {"log_level", "log_message"}

    def name(self) -> str:
        return "Log Processor"

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, list):
            return all(
                isinstance(item, dict) and self._REQUIRED_KEYS.issubset(item)
                for item in data
            )
        return False

    def ingest(self, data: list[dict[str, typing.Any]]) -> None:
        for item in data:
            self._buffer.append(": ".join(str(v) for v in item.values()))
            self._total_processed += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
                    break
            if not handled:
                print("DataStream error - Can't process"
                      f" element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(f"{proc.name()}: total {proc.total_processed} items"
                  f" processed, remaining {proc.remaining} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    ds = DataStream()
    ds.print_processors_stats()
    batch: list[typing.Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO",
                "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print("\nRegistering Numeric Processor")
    ds.register_processor(NumericProcessor())
    print(f"\nSend first batch of data on stream: {batch}")
    ds.process_stream(batch)
    ds.print_processors_stats()
    print("\nRegistering other data processors")
    ds.register_processor(TextProcessor())
    ds.register_processor(LogProcessor())
    print("Send the same batch again")
    ds.process_stream(batch)
    ds.print_processors_stats()

    numeric_proc = ds._processors[0]
    text_proc = ds._processors[1]
    log_proc = ds._processors[2]

    print("\nConsume some elements from the data"
          " processors: Numeric 3, Text 2, Log 1")
    for _ in range(3):
        numeric_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()
    ds.print_processors_stats()


if __name__ == "__main__":
    main()

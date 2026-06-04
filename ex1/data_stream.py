# should i raise exceptions instead of printing error
# messages? i think it would be better to raise
# exceptions instead of printing error messages
# but then i should create try-except
from typing import Any
import abc


class DataProcessor(abc.ABC):
    def __init__(self):
        self.data = None

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        if data is None:
            return False
        return True

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        if self.validate(data):
            self.data = data
        else:
            print(" Got exception: Invalid data type")

    def output(self) -> tuple[int, str]:
        return self.data


class NumericProcessor(DataProcessor):
    def __init__(self):
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data):
            self.data = data
        else:
            print(" Got exception: Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self):
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            self.data = data
        else:
            print(" Got exception: Improper text data")


class LogProcessor(DataProcessor):
    def __init__(self):
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(key, str) for key in data)
        if isinstance(data, list):
            return all(isinstance(item, dict)
                       and all(isinstance(key, str)
                               for key in item) for item in data)
        return False

    def ingest(self, data: dict[str, Any] | list[dict[str, Any]]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                self.data = [": ".join(str(value) for value in item.values())
                             for item in data]
            else:
                self.data = ": ".join(str(value) for value in data.values())
        else:
            print(" Got exception: Improper log data")


class DataStream:
    def __init__(self, stream):
        def register_processor(self, proc: DataProcessor) -> None:
    

def main():
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    np = NumericProcessor()
    print(f" Trying to validate input '42': {np.validate(42)}")
    print(f" Trying to validate input 'Hello': {np.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    np.ingest('foo')
    print(" Processing data: [1, 2, 3, 4, 5]")
    np.ingest([1, 2, 3, 4, 5])
    print(" Extracting 3 values...")
    print(f" Numeric value 0: {np.output()[0]}\n Numeric value"
          f" 1: {np.output()[1]}\n Numeric value 2: {np.output()[2]}\n")
    print("Testing Text Processor...")
    tp = TextProcessor()
    print(f" Trying to validate input '42': {tp.validate(42)}")
    print(" Processing data: ['Hello', 'Nexus', 'World']")
    tp.ingest(['Hello', 'Nexus', 'World'])
    print(" Extracting 1 value...")
    print(f" Text value 0: {tp.output()[0]}\n")
    print("Testing Log Processor...")
    lp = LogProcessor()
    print(f" Trying to validate input 'Hello': {lp.validate('Hello')}")
    print(" Processing data: [{'log_level': 'NOTICE',"
          " 'log_message': 'Connection to server'},"
          " {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]")
    lp.ingest([{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
              {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}])
    print(" Extracting 2 values...")
    print(f" Log entry 0: {lp.output()[0]}\n Log entry 1: {lp.output()[1]}")


if __name__ == "__main__":
    main()

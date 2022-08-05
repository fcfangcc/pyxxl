from typing import Any


class XXLRegisterError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class JobDuplicateError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class JobNotFoundError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class JobParamsError(Exception):
    def __init__(self, message: str, **kwargs: Any) -> None:
        self.message = message + ", ".join(["[%s=%s]" % (k, v) for k, v in kwargs.items()])
        super().__init__(message)


class JobRegisterError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class ClientError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

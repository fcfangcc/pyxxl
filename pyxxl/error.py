class XXLRegisterError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class JobDuplicateError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class JobNotFoundError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class JobRegisterError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class ClientError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)

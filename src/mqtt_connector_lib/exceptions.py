class MyMqttBaseError(Exception):
    """
    Base exception for all errors in the MQTT library.
    """

    def __init__(self, message, reason_code=None, **kwargs):
        # Full details are passed here for logging/debugging
        self.message = message
        self.reason_code = reason_code  # Useful for MQTT-specific codes
        self.details = kwargs  # Capture any extra relevant data

        # Pass a basic, useful string to the standard Exception constructor
        # for clean logging/display.
        super().__init__(f"{message} (Reason Code: {reason_code})" if reason_code else message)

    def __str__(self):
        # This provides the full detail when printed
        detail_str = ", ".join(f"{k}: {v}" for k, v in self.details.items())
        if self.reason_code:
            return f"[{self.__class__.__name__}] {self.message} | Code: {self.reason_code} | Details: {detail_str}"
        return f"[{self.__class__.__name__}] {self.message} | Details: {detail_str}"

    def __repr__(self):
        # A clearer, more unambiguous output for debugging
        return (f"{self.__class__.__name__}(message='{self.message}', "
                f"reason_code={self.reason_code}, details={self.details})")

class MyMqttConnectionError(MyMqttBaseError):
    """Raised when the connection fails."""
    def __init__(self, message="Failed to connect MQTT broker", reason_code=None, **kwargs):
        self.message = message
        self.reason_code = reason_code
        self.details = kwargs
        super().__init__(message, reason_code, **kwargs)



class MyMqttDisConnectionError(MyMqttBaseError):
    """Raised when the connection fails."""
    def __init__(self, message="Failed to dis-connect MQTT broker", reason_code=None, **kwargs):
        self.message = message
        self.reason_code = reason_code
        self.details = kwargs
        super().__init__(message, reason_code, **kwargs)


class MyMqttPublisherError(MyMqttBaseError):
    """Raised when the connection fails."""
    pass

class MyMqttSubscriberError(MyMqttBaseError):
    """Raised when the connection fails."""
    pass
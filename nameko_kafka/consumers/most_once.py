"""
    At most once semantic Kafka message consumer
"""

from .base import BaseConsumer


class AtMostOnceConsumer(BaseConsumer):
    """
        At most once semantic Kafka message consumer.

        :param topics: list of kafka topics to consume
        :param poll_timeout_ms: milliseconds spent waiting in poll
            if data is not available in the buffer. Default is
            100 ms.
        :param kwargs: additional kafka consumer configurations as
            keyword arguments
    """

    CONFIG = {
        # Setting auto commit off as we will manually perform offset commits
        "enable_auto_commit": False
    }

    def __init__(self, *topics, poll_timeout_ms=None, **kwargs):
        # Override passed consumer config with correct parameters
        for option, value in self.CONFIG.items():
            kwargs[option] = value
        super(AtMostOnceConsumer, self).__init__(*topics, **kwargs)
        self._poll_timeout_ms = float(poll_timeout_ms) if poll_timeout_ms else 100.0

    def start(self, callback):
        """
            Polls for new messages from broker and then commits offset
            before processing the messages. Committing before processing
            ensures at least once delivery semantic.

            :param callback: message handler callback
        """
        while True:
            messages = self.poll(timeout_ms=self._poll_timeout_ms)

            # Commits the latest offsets returned by poll
            self.commit()

            for topic, partition in messages.items():
                for message in partition:
                    # Invoke message handler to process message
                    callback(message)

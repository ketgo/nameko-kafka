"""
    At least once semantic Kafka message consumer
"""

import logging

from .base import BaseConsumer

Log = logging.getLogger(__name__)


class AtLeastOnceConsumer(BaseConsumer):
    """
        At least once semantic Kafka message consumer.

        :param topics: list of kafka topics to consume
        :param poll_timeout_ms: milliseconds spent waiting in poll
            if data is not available in the buffer. Default is
            `float("inf")` resulting in always waiting.
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
        super(AtLeastOnceConsumer, self).__init__(*topics, **kwargs)
        self._poll_timeout_ms = float(poll_timeout_ms) if poll_timeout_ms else float("inf")

    def start(self, callback):
        """
            Start consuming messages

            :param callback: message handler callback
        """
        try:
            self._start(callback)
        except Exception:
            self.close()
            raise

    def _start(self, callback):
        """
            Polls for new messages from broker and then commits offset
            after processing the messages. Committing after processing
            ensures at least once delivery semantic.

            :param callback: message handler callback
        """
        while True:
            # By default this performs blocking poll since `consumer_timeout_ms`
            # is by default set to `float("inf")`. For non-blocking poll set
            # this parameter to finite value.
            messages = self.poll(timeout_ms=self._poll_timeout_ms)

            # Break while loop if no messages are retrieved from broker. This
            # will only happen in case of non-blocking poll as blocking poll
            # only returns when messages are retrieved.
            if not messages:
                break

            for topic, partition in messages.items():
                for message in partition:
                    # Invoke message handler to process message
                    callback(message)

            # Commits the latest offsets returned by poll
            self.commit()

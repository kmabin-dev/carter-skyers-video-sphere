"""Fan producer: reads or generates a shard, writes to a temp
file and enqueues its path.

Key behaviors:
    - Optionally uses a provided shard path (for tests) or picks a
      random shard id.
    - Writes shard bytes to a temp file under `config.TEMP_DIR`.
    - Retries enqueueing with backpressure logging.
    - Registers failed temp files for cleanup worker if enqueueing
      ultimately fails.
"""

import os
import random
import tempfile
import time

from contextlib import suppress

try:
    # Optional dependency: if unavailable, we fall back to a static name list
    from faker import Faker  # type: ignore
except ImportError:
    Faker = None  # type: ignore

import config
from config import logger

if Faker is not None:
    FAKE = Faker()
else:
    FAKE = None  # type: ignore
    _FALLBACK_NAMES = [
        "Alex Johnson",
        "Taylor Reed",
        "Jordan Lee",
        "Casey Morgan",
        "Riley Parker",
        "Quinn Carter",
        "Avery Brooks",
        "Shawn Rivera",
        "Charlie Kim",
        "Robin Bailey",
        "Jessie Scott",
        "Sam Kelly",
    ]

    def _fallback_name():
        return random.choice(_FALLBACK_NAMES)


class Fan(object):
    """
    fan class
    """

    def __init__(self, fan_id, shard_path=None, verbose=True):
        self.__id = fan_id
        self.__name = FAKE.name() if FAKE else _fallback_name()
        # optional shard_path; if provided, read_random_shard will use it
        self.__shard_path = shard_path
        # keep a small buffer attribute for tests that expect it
        self.__buffer = b""
        # control whether this fan emits INFO logs (else, downgrade to
        # DEBUG)
        self.__verbose = verbose

    def id(self):
        return self.__id

    def name(self):
        return self.__name

    def buffer(self):
        return self.__buffer

    def read_random_shard(self):
        """
        Read a random shard from disk, returns the byte data.
        """
        # If a specific shard path was provided, use it.
        if getattr(self, "_Fan__shard_path", None):
            file_path = self.__shard_path
        else:
            shard_id = random.randint(0, config.NUM_SHARDS - 1)
            padded = str(shard_id).zfill(4)
            # SHARDS_DIR may be a Path; construct path safely
            file_path = config.SHARDS_DIR / f"shard_{padded}.mp4"

        try:
            with open(str(file_path), "rb") as file:
                return file.read()
        except (FileNotFoundError, PermissionError, IsADirectoryError) as e:
            # If shards are not present, generate a small dummy payload
            logger.debug(
                "Unable to open %s exception=%s; using dummy shard",
                file_path,
                type(e).__name__,
            )
            dummy = f"dummy-shard-{self.__id}-{random.randint(0, 9999)}"
            return bytes(dummy, "utf-8")

        # Should not reach here due to returns in try/except
        # but keep a defensive return of empty bytes
        return b""

    def send_shard(self, shared_buffer):
        """
        Example code to send a shard to shared buffer element 0.
        """
        # Write our shard to a temp file and publish its path to the
        # bounded queue.
        try:
            # ensure temp dir exists
            tmp_dir = config.TEMP_DIR
            os.makedirs(tmp_dir, exist_ok=True)

            # write a temporary file then publish its path
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(tmp_dir))
            payload = self.read_random_shard()
            tmp.write(payload)
            tmp.flush()
            tmp_name = tmp.name
            tmp.close()

            # If the VideoJockey has already claimed all shards, stop and
            # remove our temp file to avoid unnecessary work.
            vj_flag = getattr(shared_buffer, "vj_has_all_shards", None)
            has_all = (
                bool(getattr(vj_flag, "value", False))
                if vj_flag is not None
                else False
            )
            if has_all:
                log_fn = logger.info if self.__verbose else logger.debug
                log_fn(
                    "fan %s detected DJ has capacity/full; removing temp and exiting -> %s",
                    self.name(),
                    tmp_name,
                )
                with suppress(FileNotFoundError, PermissionError, OSError):
                    os.remove(tmp_name)
                return

            # Try to put into the bounded shared buffer with retries/backoff.
            max_attempts = 5
            attempt = 0
            put_ok = False
            while attempt < max_attempts and not put_ok:
                # block up to 2 seconds to allow DJ to consume
                put_ok = shared_buffer.put_shard(
                    self.__name, tmp_name, timeout=2.0
                )
                if not put_ok:
                    logger.debug(
                        "fan %s backpressure: buffer full, retrying (%d/%d)",
                        self.name(),
                        attempt + 1,
                        max_attempts,
                    )
                    time.sleep(0.2 * (attempt + 1))
                attempt += 1

            if put_ok:
                log_fn = logger.info if self.__verbose else logger.debug
                log_fn("The fan %s sent shard -> shared buffer", self.name())
            else:
                logger.error(
                    "fan %s failed to enqueue shard after %d attempts; registering shard for later cleanup %s",
                    self.name(),
                    max_attempts,
                    tmp_name,
                )
                # Register the temp file with the shared buffer failed-temp
                # list; fall back to best-effort removal if registration fails
                register = getattr(shared_buffer, "register_failed_temp", None)
                if callable(register):
                    try:
                        register(tmp_name)
                    except (
                        AttributeError,
                        ValueError,
                        TypeError,
                        EOFError,
                        BrokenPipeError,
                    ) as e:
                        logger.debug("register_failed_temp failed: %s", e)
                        with suppress(
                            FileNotFoundError, PermissionError, OSError
                        ):
                            os.remove(tmp_name)
                else:
                    with suppress(FileNotFoundError, PermissionError, OSError):
                        os.remove(tmp_name)

        except (OSError, IOError) as e:
            logger.error("fan %s failed to write shard: %s", self.name(), e)

    def start(self, shared_buffer):
        self.send_shard(shared_buffer)

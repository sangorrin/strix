import os
import inspect
from datetime import datetime, timezone
from typing import Any, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError

class MongoDBLogger:
    """
    Centralized MongoDB logger for distributed Strix agents.
    Thread-safe and connection-pooled via PyMongo.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        database: str = "strix_logs",
        collection: str = "logs"
    ):
        """
        Initialize MongoDB logger.

        Args:
            uri: MongoDB connection string (defaults to MONGODB_URI env var)
            database: Database name
            collection: Collection name
        """
        self.uri = uri or os.getenv("MONGODB_URI")
        if not self.uri:
            raise ValueError("MongoDB URI not provided. Set MONGODB_URI env variable.")

        # Strip credentials from URI for safe logging
        safe_uri = self.uri.split("@")[-1] if "@" in self.uri else self.uri
        print(f"Initializing MongoDB logger -> {safe_uri}")

        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[database]

            # Verify connection
            self.client.admin.command('ping')

            # Create time series collection if it doesn't exist
            existing_collections = self.db.list_collection_names()
            if collection not in existing_collections:
                print(f"Creating time series collection: {database}.{collection}")
                self.db.create_collection(
                    collection,
                    timeseries={
                        "timeField": "timestamp",
                        "metaField": "metadata",
                        "granularity": "seconds"
                    },
                    expireAfterSeconds=60*60*24*90  # 90-day TTL
                )

            self.collection = self.db[collection]

            # Create indexes for efficient querying
            try:
                self.collection.create_index([("metadata.user_id", 1)])
                self.collection.create_index([("metadata.run_id", 1)])
                self.collection.create_index([("metadata.level", 1)])
                self.collection.create_index([("metadata.agent_id", 1), ("timestamp", -1)])
                self.collection.create_index([("metadata.agent_name", 1), ("timestamp", -1)])
            except PyMongoError:
                pass  # Indexes may already exist or collection is time series

            print(f"✓ Connected to MongoDB: {database}.{collection}")
        except PyMongoError as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

    def log(
        self,
        content: Any,
        run_id: str,
        agent_id: str,
        agent_name: str = "unknown",
        level: str = "INFO",
        user_id: str = "default_user"
    ) -> None:
        """
        Log a message to MongoDB.

        Args:
            content: The log content (any JSON-serializable type)
            run_id: Unique identifier for the program execution
            agent_id: Identifier for the specific agent/thread
            agent_name: Human-readable name of the agent
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            user_id: User identifier for multi-tenant logging
        """
        # Get caller information
        stack = inspect.stack()
        for frame_info in stack:
            filename = frame_info.filename
            if "mongodb_logger.py" not in filename:
                module = frame_info.frame.f_globals.get("__name__", filename)
                function = frame_info.function
                lineno = frame_info.lineno
                calling_id = f"{module}:{function}:{lineno}"
                break
        else:
            # Fallback to previous behavior
            calling_id = f"{stack[1].filename}:{stack[1].function}:{stack[1].lineno}"

        # Time series format: timestamp + metadata
        log_entry = {
            "timestamp": datetime.now(timezone.utc),
            "metadata": {
                "user_id": user_id,
                "run_id": run_id,
                "agent_id": agent_id,
                "agent_name": agent_name,
                "calling_id": calling_id,
                "level": level,
            },
            "content": content
        }

        try:
            self.collection.insert_one(log_entry)
        except PyMongoError as e:
            # Fallback to stderr if MongoDB fails (non-blocking)
            import sys
            print(f"[MONGODB_LOG_ERROR] {e}: {log_entry}", file=sys.stderr)

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()


# Singleton instance for easy import
_logger_instance: Optional[MongoDBLogger] = None

def get_logger(run_id: str, agent_id: str, agent_name: str = "unknown", user_id: str = "default_user") -> 'LoggerProxy':
    """
    Get or create MongoDB logger instance.

    Args:
        run_id: Unique execution identifier
        agent_id: Agent/thread identifier
        agent_name: Human-readable agent name
        user_id: User identifier for multi-tenant logging

    Returns:
        LoggerProxy with run_id/agent_id/agent_name/user_id bound
    """
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = MongoDBLogger()

    return LoggerProxy(_logger_instance, run_id, agent_id, agent_name, user_id)


class LoggerProxy:
    """Proxy to bind run_id, agent_id, agent_name, and user_id to logger calls."""

    def __init__(self, logger: MongoDBLogger, run_id: str, agent_id: str, agent_name: str = "unknown", user_id: str = "default_user"):
        self._logger = logger
        self._run_id = run_id
        self._agent_id = agent_id
        self._agent_name = agent_name
        self._user_id = user_id

    def debug(self, content: Any):
        self._logger.log(content, self._run_id, self._agent_id, self._agent_name, "DEBUG", self._user_id)

    def info(self, content: Any):
        self._logger.log(content, self._run_id, self._agent_id, self._agent_name, "INFO", self._user_id)

    def warning(self, content: Any):
        self._logger.log(content, self._run_id, self._agent_id, self._agent_name, "WARNING", self._user_id)

    def error(self, content: Any):
        self._logger.log(content, self._run_id, self._agent_id, self._agent_name, "ERROR", self._user_id)

    def critical(self, content: Any):
        self._logger.log(content, self._run_id, self._agent_id, self._agent_name, "CRITICAL", self._user_id)


# Example usage
if __name__ == "__main__":
    import sys
    logger = get_logger(run_id="test_run_001", agent_id="agent_alpha")
    logger.info("Agent started successfully")
    logger.error({"error": "Connection timeout", "retry_count": 3})

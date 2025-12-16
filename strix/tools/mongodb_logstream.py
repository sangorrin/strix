#!/usr/bin/env python3
"""
MongoDB Log Streaming CLI Tool for Strix

Real-time log streaming from MongoDB with filtering and polling capabilities.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDBLogStreamer:
    """Stream logs from MongoDB in real-time."""

    def __init__(
        self,
        uri: Optional[str] = None,
        database: str = "strix_logs",
        collection: str = "logs"
    ):
        """
        Initialize MongoDB log streamer.

        Args:
            uri: MongoDB connection string (defaults to MONGODB_URI env var)
            database: Database name
            collection: Collection name
        """
        self.uri = uri or os.getenv("MONGODB_URI")
        if not self.uri:
            raise ValueError("MongoDB URI not provided. Set MONGODB_URI env variable.")

        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[database]
            self.collection = self.db[collection]

            # Verify connection
            self.client.admin.command('ping')
            print(f"✓ Connected to MongoDB: {database}.{collection}", file=sys.stderr)
        except PyMongoError as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

        self.last_timestamp: Optional[datetime] = None

    def fetch_logs(
        self,
        run_id: str,
        event_filter: Optional[list[str]] = None,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        agent_name: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Fetch new log entries since last fetch.

        Args:
            run_id: Run ID to filter logs
            event_filter: Optional list of event types to filter
            user_id: Optional user ID to filter logs
            agent_id: Optional agent ID to filter logs
            agent_name: Optional agent name to filter logs

        Returns:
            List of log entries
        """
        query: dict[str, Any] = {
            "metadata.run_id": run_id
        }

        # Filter by user_id if specified
        if user_id:
            query["metadata.user_id"] = user_id

        # Filter by agent_id if specified
        if agent_id:
            query["metadata.agent_id"] = agent_id

        # Filter by agent_name if specified
        if agent_name:
            query["metadata.agent_name"] = agent_name

        # Only fetch entries newer than last timestamp
        if self.last_timestamp:
            query["timestamp"] = {"$gt": self.last_timestamp}

        # Filter by event types if specified
        if event_filter:
            query["content.event"] = {"$in": event_filter}

        try:
            # Sort by timestamp to ensure chronological order
            cursor = self.collection.find(query).sort("timestamp", 1)
            logs = list(cursor)

            # Update last timestamp to the most recent entry
            if logs:
                self.last_timestamp = logs[-1]["timestamp"]

            return logs
        except PyMongoError as e:
            print(f"Error fetching logs: {e}", file=sys.stderr)
            return []

    def format_log_entry(self, log_entry: dict[str, Any]) -> str:
        """
        Format a log entry for display.

        Args:
            log_entry: MongoDB log entry

        Returns:
            Formatted string
        """
        # Extract fields
        timestamp = log_entry.get("timestamp")
        metadata = log_entry.get("metadata", {})
        content = log_entry.get("content", {})

        # Format timestamp
        if isinstance(timestamp, datetime):
            ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            ts_str = str(timestamp)

        # Build formatted output
        output = {
            "timestamp": ts_str,
            "user_id": metadata.get("user_id", "default_user"),
            "agent_id": metadata.get("agent_id", "unknown"),
            "agent_name": metadata.get("agent_name", "unknown"),
            "level": metadata.get("level", "INFO"),
            "content": content
        }

        return json.dumps(output, indent=2, default=str)

    def stream_logs(
        self,
        run_id: str,
        event_filter: Optional[list[str]] = None,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        agent_name: Optional[str] = None,
        polling_interval: int = 3
    ) -> None:
        """
        Stream logs in real-time with polling.

        Args:
            run_id: Run ID to stream
            event_filter: Optional event type filter
            user_id: Optional user ID filter
            agent_id: Optional agent ID filter
            agent_name: Optional agent name filter
            polling_interval: Polling interval in seconds
        """
        print(f"Streaming logs for run_id: {run_id}", file=sys.stderr)
        if user_id:
            print(f"User ID filter: {user_id}", file=sys.stderr)
        if agent_id:
            print(f"Agent ID filter: {agent_id}", file=sys.stderr)
        if agent_name:
            print(f"Agent name filter: {agent_name}", file=sys.stderr)
        if event_filter:
            print(f"Event filter: {', '.join(event_filter)}", file=sys.stderr)
        print(f"Polling interval: {polling_interval}s", file=sys.stderr)
        print(f"Press Ctrl+C to stop\n", file=sys.stderr)

        try:
            while True:
                logs = self.fetch_logs(run_id, event_filter, user_id, agent_id, agent_name)

                for log in logs:
                    print(self.format_log_entry(log))
                    print()  # Blank line between entries

                # Wait before next poll
                time.sleep(polling_interval)

        except KeyboardInterrupt:
            print("\nStopping log stream...", file=sys.stderr)
        finally:
            self.close()

    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()


def main() -> None:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Stream MongoDB logs for Strix penetration testing runs in real-time",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream all logs for a run
  python mongodb_logstream.py --run-name my-scan-run

  # Stream logs for a specific user
  python mongodb_logstream.py --run-name my-scan-run --user-id alice

  # Stream only vulnerability-related events
  python mongodb_logstream.py --run-name my-scan-run --events vuln_plan,vuln_step_start,vuln_step_complete,vulnerability_found

  # Custom polling interval
  python mongodb_logstream.py --run-name my-scan-run --polling 5

  # Stream specific events for a user with custom polling
  python mongodb_logstream.py --run-name my-scan-run --user-id alice --events agent_creation,tool_execution_start --polling 1
        """
    )

    parser.add_argument(
        "--run-name",
        required=True,
        help="Run name (run_id) to stream logs from"
    )

    parser.add_argument(
        "--user-id",
        default="default_user",
        help="User ID to filter logs (default: default_user)"
    )

    parser.add_argument(
        "--agent-id",
        help="Agent ID to filter logs (e.g., agent_a1b2c3d4)"
    )

    parser.add_argument(
        "--agent-name",
        help="Agent name to filter logs (e.g., 'Strix Agent', 'Vulnerability Scanner')"
    )

    parser.add_argument(
        "--events",
        help="Comma-separated list of event types to filter (default: all events). "
             "Available events: live_stats_update, vuln_plan, vulnerability_found, scan_complete, "
             "agent_creation, tool_execution_start, tool_execution_complete, agent_status_update, "
             "scan_config_set, llm_request, llm_response"
    )

    parser.add_argument(
        "--polling",
        type=int,
        default=3,
        help="Polling interval in seconds (default: 3)"
    )

    parser.add_argument(
        "--database",
        default="strix_logs",
        help="MongoDB database name (default: strix_logs)"
    )

    parser.add_argument(
        "--collection",
        default="logs",
        help="MongoDB collection name (default: logs)"
    )

    args = parser.parse_args()

    # Parse event filter
    event_filter = None
    if args.events:
        event_filter = [e.strip() for e in args.events.split(",") if e.strip()]

    try:
        # Create streamer
        streamer = MongoDBLogStreamer(
            database=args.database,
            collection=args.collection
        )

        # Start streaming
        streamer.stream_logs(
            run_id=args.run_name,
            event_filter=event_filter,
            user_id=args.user_id,
            agent_id=args.agent_id,
            agent_name=args.agent_name,
            polling_interval=args.polling
        )

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        print("\nMake sure MONGODB_URI environment variable is set.", file=sys.stderr)
        sys.exit(1)
    except ConnectionError as e:
        print(f"Connection Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

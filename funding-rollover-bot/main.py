#!/usr/bin/env python3
"""
Funding Rollover Super Scalp Bot
Entry point for live trading and backtesting.
"""
import argparse
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.logging_utils import setup_logging, get_logger
from core.scheduler import Scheduler
from backtest.event_replayer import EventDrivenBacktester


def main():
    parser = argparse.ArgumentParser(description="Funding Rollover Scalp Bot")
    subparsers = parser.add_subparsers(dest="command")

    # Live trading
    live_parser = subparsers.add_parser("live", help="Run live trading")
    live_parser.add_argument("--config", default="config/strategy.yaml")
    live_parser.add_argument("--dry-run", action="store_true")
    live_parser.add_argument("--log-level", default="INFO")

    # Backtest
    bt_parser = subparsers.add_parser("backtest", help="Run backtest")
    bt_parser.add_argument("--config", default="config/strategy.yaml")
    bt_parser.add_argument("--symbols", nargs="+", default=["BTCUSDT"])
    bt_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    bt_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    bt_parser.add_argument("--log-level", default="INFO")

    # UI dashboard
    ui_parser = subparsers.add_parser("ui", help="Run web dashboard UI server")
    ui_parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    ui_parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    ui_parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()

    if args.command == "live":
        setup_logging(args.log_level)
        logger = get_logger("main")
        logger.info("Starting live trading bot", dry_run=args.dry_run)
        scheduler = Scheduler(config_path=args.config, dry_run=args.dry_run)
        asyncio.run(scheduler.run_forever())

    elif args.command == "backtest":
        setup_logging(args.log_level)
        logger = get_logger("main")
        logger.info("Starting backtest", symbols=args.symbols, start=args.start, end=args.end)
        bt = EventDrivenBacktester(config_path=args.config)
        result = bt.run(symbols=args.symbols, start_date=args.start, end_date=args.end)
        from backtest.metrics import BacktestMetrics
        BacktestMetrics.print_report(result)

    elif args.command == "ui":
        setup_logging(args.log_level)
        logger = get_logger("main")
        logger.info("Starting dashboard UI server", host=args.host, port=args.port)
        # Import here to keep UI deps optional for bot-only installs
        from ui.server import run_server
        run_server(host=args.host, port=args.port)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()

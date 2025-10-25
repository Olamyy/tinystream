import asyncio

from tinystream.broker import Broker


def main():
    broker = Broker()

    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        print("\nBroker shutting down.")


if __name__ == "__main__":
    main()

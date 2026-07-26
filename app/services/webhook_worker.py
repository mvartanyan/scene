from app.services.webhooks import run_worker_forever


def main() -> None:
    run_worker_forever()


if __name__ == "__main__":
    main()

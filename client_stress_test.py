
import socket
import time

from concurrent.futures import ThreadPoolExecutor
from protocol import recv_message, encode_message


HOST = "127.0.0.1"
PORT = 9090

NUM_CLIENTS = 20


def run_client(worker_id):
    """Store a value and check that we can get it back."""

    key = f"worker_{worker_id}"
    value = f"value_{worker_id}"

    try:
        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        ) as client_sock:

            # Connect to the server
            client_sock.connect((HOST, PORT))

            # Store a value
            put_request = {
                "command": "PUT",
                "key": key,
                "value": value
            }

            client_sock.sendall(
                encode_message(put_request)
            )

            put_response = recv_message(client_sock)

            if put_response.get("status") != "ok":
                raise AssertionError(
                    f"PUT failed: {put_response!r}"
                )

            # Get the value back
            get_request = {
                "command": "GET",
                "key": key
            }

            client_sock.sendall(
                encode_message(get_request)
            )

            get_response = recv_message(client_sock)

            if get_response.get("status") != "ok":
                raise AssertionError(
                    f"GET failed: {get_response!r}"
                )

            if get_response.get("value") != value:
                raise AssertionError(
                    f"GET mismatch: expected {value!r}, "
                    f"received {get_response!r}"
                )

            print(
                f"[Worker Thread {worker_id:02d}] "
                "Transaction verified successfully."
            )

    except Exception as error:
        print(
            f"[Worker Thread {worker_id:02d}] "
            f"FAILED: {error}"
        )
        raise


def main():
    print(
        f"[*] Launching {NUM_CLIENTS} "
        "concurrent client worker threads..."
    )

    start_time = time.perf_counter()

    errors = []

    # Run 20 clients at once
    with ThreadPoolExecutor(
        max_workers=NUM_CLIENTS
    ) as executor:

        futures = [
            executor.submit(run_client, worker_id)
            for worker_id in range(NUM_CLIENTS)
        ]

        errors = []

        for future in futures:
            try:
                future.result()
            except Exception as error:
                errors.append(error)

    elapsed = time.perf_counter() - start_time

    if errors:
        print(
            f"[-] Test failed: {len(errors)} "
            "client transaction(s) failed."
        )
        raise SystemExit(1)

    print(
        f"[+] All {NUM_CLIENTS} workers completed "
        f"execution in {elapsed:.3f} seconds."
    )


if __name__ == "__main__":
    main()

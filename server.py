
import socket
from concurrent.futures import ThreadPoolExecutor

from protocol import recv_message, encode_message
from storage import KeyValueStore, TaskQueue


HOST = "127.0.0.1"
PORT = 9090
MAX_WORKERS = 20

# Shared data for all clients
store = KeyValueStore()
task_queue = TaskQueue()


def handle_client(client_sock, client_address):
    """Handle requests from a client."""

    print(f"Client connected: {client_address}")

    try:
        with client_sock:
            while True:
                message = recv_message(client_sock)
                print(f"Received from {client_address}: {message!r}")

                command = message.get("command")

                if command == "PUT":
                    store.put(message["key"], message["value"])

                    response = {
                        "status": "ok",
                        "message": "Value stored"
                    }

                elif command == "GET":
                    value = store.get(message["key"])

                    response = {
                        "status": "ok",
                        "value": value
                    }

                elif command == "ENQUEUE":
                    task_queue.enqueue(message["task"])

                    response = {
                        "status": "ok",
                        "message": "Task added to queue"
                    }

                elif command == "DEQUEUE":
                    task = task_queue.dequeue()

                    response = {
                        "status": "ok",
                        "task": task
                    }

                else:
                    response = {
                        "status": "error",
                        "message": "Unknown command"
                    }

                client_sock.sendall(encode_message(response))

    except (ConnectionError, OSError) as exc:
        print(f"Client disconnected: {client_address}: {exc}")

    except (KeyError, TypeError, ValueError) as exc:
        print(f"Invalid request from {client_address}: {exc}")

    except Exception as exc:
        print(f"Error handling {client_address}: {exc}")

    finally:
        print(f"Connection closed: {client_address}")


def start_server():
    """Start the server and accept connections."""

    server_sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server_sock.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server_sock.bind((HOST, PORT))
    server_sock.listen()

    print(f"Server listening on {HOST}:{PORT}")
    print(f"Maximum worker threads: {MAX_WORKERS}")

    try:
        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            while True:
                client_sock, client_address = server_sock.accept()

                executor.submit(
                    handle_client,
                    client_sock,
                    client_address
                )

    except KeyboardInterrupt:
        print("\nServer shutting down.")

    finally:
        server_sock.close()


if __name__ == "__main__":
    start_server()

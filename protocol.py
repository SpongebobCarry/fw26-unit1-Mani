import json
import struct
import socket


HEADER_SIZE = 4
MAX_PAYLOAD_SIZE = 1024 * 1024  # 1 MB limit


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read the full number of bytes from the socket."""

    data = bytearray()

    while len(data) < n:
        chunk = sock.recv(n - len(data))

        if not chunk:
            raise ConnectionError("Socket closed while receiving data")

        data.extend(chunk)

    return bytes(data)


def encode_message(message: dict) -> bytes:
    """Convert a dictionary into bytes with a size header."""

    # Convert the data to JSON
    json_string = json.dumps(message)

    # Turn it into bytes
    payload = json_string.encode("utf-8")

    # Check the message size
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload is too large")

    # Add the 4-byte header
    header = struct.pack("!I", len(payload))

    # Send the header and message together
    return header + payload


def recv_message(sock: socket.socket) -> dict:
    """Read a message from the socket."""

    # Read the header
    header = recv_exact(sock, HEADER_SIZE)

    # Get the message length
    payload_length = struct.unpack("!I", header)[0]

    # Check the message size
    if payload_length > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload is too large")

    # Read the message
    payload = recv_exact(sock, payload_length)

    # Convert bytes to text
    json_string = payload.decode("utf-8")

    # Convert JSON back to a dictionary
    message = json.loads(json_string)

    return message
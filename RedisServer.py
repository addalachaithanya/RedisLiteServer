import socket
import threading
import time


# *3\r\n$3\r\nset\r\n$3\r\nName\r\n$4\r\njohn\r\n
#
data_ids = ['+', '*', '$', '-']
RDB = {}

#Return Simple String for Ping and Echo only
def handle_ping():
    # RESP protocol for PING response is "+PONG\r\n"
    response = f"{data_ids[0]}PONG\\r\\n".encode()
    return response

#Return Simple String for Ping and Echo only
def handle_echo_command(message):
    # Extract the message after "ECHO " (case insensitive)
    message_parts = message.split(maxsplit=1)
    if len(message_parts) > 1:
        echo_message = message_parts[1]
        response = f"{data_ids[0]}{echo_message}\\r\\n".encode()
        return response
    else:
        return b"-ERR invalid ECHO command\r\n"

#Return Simple string for Set Command
def handle_set_command(message):
    message_parts = message.split()
    if len(message_parts) != 3:
        response = f"{data_ids[-1]}Error message\\r\\n".encode()
    message_parts = message.split()
    RDB[message_parts[1].upper()] = message_parts[2].upper()
    response = f"{data_ids[0]}OK\\r\\n".encode()

    return response

#Return Bulk String for Get Command
def handle_get_command(message):
    message_parts = message.split()
    print(RDB)
    if message_parts[-1].upper() not in RDB.keys():
        response = f"{data_ids[-1]}Error message\\r\\n".encode()
    else:
        value = RDB[message_parts[-1].upper()]
        response = f"{data_ids[2]}{len(value)}\\r\\n{value}\\r\\n".encode()
    return response

def handle_redis_clients(client_socket, timeout):
    try:
        buffer_size = 65536  # Size of the buffer to receive data
        accumulated_data = b""
        start_time = time.time()  # Initialize start_time here
        
        message_to_client = b""

        while True:
            try:
                # Receive data from the client in chunks
                data = client_socket.recv(buffer_size)
                
                if not data:
                    print("No data received; client might have closed the connection.")
                    break  # Break the loop if no data is received (client closed connection)
                        
                accumulated_data += data
                                      
                # Process the entire message and clear the buffer
                if b'\n' in accumulated_data:
                    messages = accumulated_data.split(b'\n')
                    
                    # Process the complete message(s)
                    for message in messages[:-1]:
                        decoded_message = message.decode().strip()
                        time_start = time.time()
                        print(f"Received From Client: {decoded_message}")
                        if decoded_message.upper().startswith("PING"):
                            response = handle_ping()
                            client_socket.sendall(response)
                            time_end = time.time() - time_start
                            client_socket.sendall(f"\r\n{time_end}\r\n".encode())

                        elif "ECHO" in decoded_message.upper():
                            response = handle_echo_command(decoded_message)
                            print("Server Response:")
                            print(response)
                            client_socket.sendall(response)
                            time_end = time.time() - time_start
                            client_socket.sendall(f"\r\n{time_end}\r\n".encode())

                        elif decoded_message.upper().startswith("SET"):
                            response = handle_set_command(decoded_message)
                            print(response)
                            client_socket.sendall(response)
                            time_end = time.time() - time_start
                            client_socket.sendall(f"\r\n{time_end}\r\n".encode())
                        
                        elif decoded_message.upper().startswith("GET"):
                            response = handle_get_command(decoded_message)
                            print(response)
                            client_socket.sendall(response)
                            time_end = time.time() - time_start
                            client_socket.sendall(f"\r\n{time_end}\r\n".encode())

                        else:
                            response = b"-ERR unknown command\r\n"
                            client_socket.sendall(response)

                    # Send acknowledgment after processing all messages
                    client_socket.sendall(b"\r\nMessage received and processed...\r\n")
                    
                    # Clear the buffer
                    accumulated_data = b""
                        
                    # Update the last activity time
                    start_time = time.time()
                    
            except socket.timeout:
                # Check for inactivity period
                if time.time() - start_time >= timeout:
                    print("Connection timed out due to inactivity.")
                    break
                    
            except ConnectionResetError:
                print("Client disconnected abruptly")
                break
                    
            except Exception as e:
                print(f"Error: {e}")
                break

    finally:
        client_socket.close()  # Ensure the socket is closed

def create_redis_server(host, port, timeout=3600):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(50)
    print(f"Server listening on {host}:{port}")

    while True:
        try:
            client_socket, client_address = server_socket.accept()
            print(f"Connection from {client_address}")
            client_socket.sendall(b"Welcome, Mr. Specter\r\n")

            # Set a timeout for blocking socket operations (for receiving data)
            client_socket.settimeout(timeout)
            client_thread = threading.Thread(target=handle_redis_clients, args=(client_socket, timeout))
            client_thread.start()       
        
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 6379
    timeout = 3600  # Timeout in seconds
    create_redis_server(host, port, timeout)

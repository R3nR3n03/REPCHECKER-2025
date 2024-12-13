import pymysql
from pymysql import MySQLError
import tkinter as tk
from tkinter import scrolledtext, messagebox
import threading


def check_connection(host, user, password, database, port, retry=False):
    """Check if the connection to MySQL is successful with optional retry mechanism."""
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=5  # Set a timeout for the connection attempt
        )
        if connection.open:
            connection.close()
            return True
        else:
            return False
    except MySQLError as e:
        print(f"Connection failed: {e}")
        if retry:
            return messagebox.askretrycancel("Connection Error",
                                             f"Failed to connect to MySQL. Error: {e}\nWould you like to retry?")
        else:
            return False


def fetch_status_for_servers(servers, connection_type="main_to_node"):
    """Fetch MySQL slave status for the provided list of servers."""
    button_check.config(state=tk.DISABLED)
    result_text.delete(1.0, tk.END)
    result_text.insert(tk.END, f"Connecting to MySQL... Please wait.\n")

    def fetch_replication_status():
        try:
            for server in servers:
                host = server.get('host')
                user = server.get('user')
                password = server.get('password')
                database = server.get('database')
                port = server.get('port', 3306)
                result_text.insert(tk.END, f"\nChecking replication status for {host}...\n")

                # Check if connection is successful
                if not check_connection(host, user, password, database, port, retry=True):
                    continue

                # Connect to MySQL and fetch replication status
                connection = pymysql.connect(
                    host=host,
                    user=user,
                    password=password,
                    database=database,
                    port=port,
                    connect_timeout=5  # Set a timeout for the connection
                )

                if connection.open:
                    cursor = connection.cursor()
                    cursor.execute("SHOW SLAVE STATUS")
                    slave_status = cursor.fetchall()
                    server_info = connection.get_server_info()

                    result_text.insert(tk.END, f"Connected to MySQL Server {server_info}\n")

                    if slave_status:
                        for row in slave_status:
                            if connection_type == "main_to_node":
                                result_text.insert(tk.END, f"Slave IO Running: {row[10]}\n")
                                result_text.insert(tk.END, f"Slave SQL Running: {row[11]}\n")
                                result_text.insert(tk.END, f"Read Master Log Pos: {row[18]}\n")
                                result_text.insert(tk.END, f"Relay Log File: {row[21]}\n")
                                result_text.insert(tk.END, f"Relay Log Pos: {row[22]}\n")
                                result_text.insert(tk.END, f"Slave IO State: {row[15]}\n")
                                result_text.insert(tk.END, f"Last Error: {row[23]}\n")
                            elif connection_type == "node_to_node":
                                result_text.insert(tk.END, f"Slave IO Running: {row[10]}\n")
                                result_text.insert(tk.END, f"Slave SQL Running: {row[11]}\n")
                                result_text.insert(tk.END, f"Read Master Log Pos: {row[18]}\n")
                                result_text.insert(tk.END, f"Relay Log File: {row[21]}\n")
                                result_text.insert(tk.END, f"Relay Log Pos: {row[22]}\n")
                                result_text.insert(tk.END, f"Slave IO State: {row[15]}\n")
                                result_text.insert(tk.END, f"Last Error: {row[23]}\n")
                            result_text.insert(tk.END, "-" * 50 + "\n")
                    else:
                        result_text.insert(tk.END, f"No replication slave status found for {host}.\n")

                    connection.close()
                else:
                    result_text.insert(tk.END, f"Failed to connect to {host}.\n")
        except MySQLError as e:
            result_text.insert(tk.END, f"MySQL error: {e}\n")
            messagebox.showerror("MySQL Error", f"Error while fetching slave status: {e}")
        except Exception as e:
            result_text.insert(tk.END, f"Unexpected error: {e}\n")
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")
        finally:
            root.after(0, lambda: button_check.config(state=tk.NORMAL))

    threading.Thread(target=fetch_replication_status, daemon=True).start()


def check_replication():
    """Check replication for both directions (Main to Node and Node to Node)."""
    # Validate main server inputs
    if not entry_host.get() or not entry_user.get() or not entry_password.get() or not entry_database.get() or not entry_port.get():
        messagebox.showerror("Input Error", "Please fill all fields for the main server.")
        return

    main_server = {
        'host': entry_host.get(),
        'user': entry_user.get(),
        'password': entry_password.get(),
        'database': entry_database.get(),
        'port': int(entry_port.get())
    }

    # Validate node server inputs
    node_servers = []
    for node in node_servers_list:
        if not node['host'].get() or not node['user'].get() or not node['password'].get() or not node[
            'database'].get() or not node['port'].get():
            messagebox.showerror("Input Error", "Please fill all fields for node server.")
            return
        node_servers.append({
            'host': node['host'].get(),
            'user': node['user'].get(),
            'password': node['password'].get(),
            'database': node['database'].get(),
            'port': int(node['port'].get())
        })

    # Check replication from Main server to Node servers
    fetch_status_for_servers([main_server] + node_servers, connection_type="main_to_node")

    # Check replication from Node server to Node server (for chaining replication)
    fetch_status_for_servers(node_servers, connection_type="node_to_node")


def add_node_server():
    """Add a new node server to the dashboard."""
    node_frame = tk.Frame(node_dashboard)
    node_frame.pack(pady=5)

    # Input fields for node server
    node_host = tk.Entry(node_frame, width=15)
    node_host.grid(row=0, column=1, padx=5)

    node_user = tk.Entry(node_frame, width=15)
    node_user.grid(row=1, column=1, padx=5)

    node_password = tk.Entry(node_frame, show="*", width=15)
    node_password.grid(row=2, column=1, padx=5)

    node_database = tk.Entry(node_frame, width=15)
    node_database.grid(row=3, column=1, padx=5)

    node_port = tk.Entry(node_frame, width=15)
    node_port.grid(row=4, column=1, padx=5)

    # Labels
    tk.Label(node_frame, text="Host:").grid(row=0, column=0)
    tk.Label(node_frame, text="User:").grid(row=1, column=0)
    tk.Label(node_frame, text="Password:").grid(row=2, column=0)
    tk.Label(node_frame, text="Database:").grid(row=3, column=0)
    tk.Label(node_frame, text="Port:").grid(row=4, column=0)

    # Store the node inputs
    node_servers_list.append({
        'host': node_host,
        'user': node_user,
        'password': node_password,
        'database': node_database,
        'port': node_port
    })

    # Display added node server in the node server dashboard
    node_display_frame = tk.Frame(node_display_dashboard)
    node_display_frame.pack(pady=5)

    tk.Label(node_display_frame, text="Host:").grid(row=0, column=0)
    tk.Label(node_display_frame, text="User:").grid(row=1, column=0)
    tk.Label(node_display_frame, text="Password:").grid(row=2, column=0)
    tk.Label(node_display_frame, text="Database:").grid(row=3, column=0)
    tk.Label(node_display_frame, text="Port:").grid(row=4, column=0)

    tk.Label(node_display_frame, text=node_host.get()).grid(row=0, column=1)
    tk.Label(node_display_frame, text=node_user.get()).grid(row=1, column=1)
    tk.Label(node_display_frame, text=node_password.get()).grid(row=2, column=1)
    tk.Label(node_display_frame, text=node_database.get()).grid(row=3, column=1)
    tk.Label(node_display_frame, text=node_port.get()).grid(row=4, column=1)

    # Clear the input fields for the next node
    node_host.delete(0, tk.END)
    node_user.delete(0, tk.END)
    node_password.delete(0, tk.END)
    node_database.delete(0, tk.END)
    node_port.delete(0, tk.END)


def clear_node_servers():
    """Clear the node servers list."""
    for widget in node_display_dashboard.winfo_children():
        widget.destroy()
    node_servers_list.clear()


# GUI setup
root = tk.Tk()
root.title("MySQL Replication Checker")

# Main server input frame
frame = tk.Frame(root)
frame.pack(padx=10, pady=10)

tk.Label(frame, text="Host:").grid(row=0, column=0)
entry_host = tk.Entry(frame)
entry_host.grid(row=0, column=1)

tk.Label(frame, text="User:").grid(row=1, column=0)
entry_user = tk.Entry(frame)
entry_user.grid(row=1, column=1)

tk.Label(frame, text="Password:").grid(row=2, column=0)
entry_password = tk.Entry(frame, show="*")
entry_password.grid(row=2, column=1)

tk.Label(frame, text="Database:").grid(row=3, column=0)
entry_database = tk.Entry(frame)
entry_database.grid(row=3, column=1)

tk.Label(frame, text="Port:").grid(row=4, column=0)
entry_port = tk.Entry(frame)
entry_port.grid(row=4, column=1)

# Buttons
button_check = tk.Button(root, text="Check Replication", command=check_replication)
button_check.pack(pady=20)

button_add_node = tk.Button(root, text="Add Node Server", command=add_node_server)
button_add_node.pack(pady=5)

button_clear_node = tk.Button(root, text="Clear Node Servers", command=clear_node_servers)
button_clear_node.pack(pady=5)

# Node Server Dashboards
node_servers_list = []
node_dashboard = tk.Frame(root)
node_dashboard.pack(padx=10, pady=10)

node_display_dashboard = tk.Frame(root)
node_display_dashboard.pack(padx=10, pady=10)

result_text = scrolledtext.ScrolledText(root, height=15, width=80)
result_text.pack(padx=10, pady=10)

root.mainloop()

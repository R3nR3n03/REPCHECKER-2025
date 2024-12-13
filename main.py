#PROTOTYPE
import pymysql
from pymysql import MySQLError
import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk
import threading
import csv
import json

# Global list to store node servers
node_servers_list = []


# Function to check the MySQL connection
def check_connection(host, user, password, database, port):
    """Check if the connection to MySQL is successful."""
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=10  # Set a timeout for the connection attempt
        )
        if connection.open:
            connection.close()
            return True
        else:
            return False
    except MySQLError as e:
        error_message = str(e)
        messagebox.showerror("Connection Error", f"Failed to connect to MySQL. Error: {error_message}")
        return False


# Fetch replication status for multiple servers
def fetch_status_for_servers(servers, connection_type="main_to_node"):
    """Fetch MySQL slave status for the provided list of servers."""
    button_check.config(state=tk.DISABLED)
    result_text.delete(1.0, tk.END)  # Clear existing results
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
                if not check_connection(host, user, password, database, port):
                    result_text.insert(tk.END, f"Connection failed to {host}.\n")
                    continue

                # Connect to MySQL and fetch replication status
                connection = pymysql.connect(
                    host=host,
                    user=user,
                    password=password,
                    database=database,
                    port=port,
                    connect_timeout=10  # Set a timeout for the connection
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
                else:
                    result_text.insert(tk.END, f"Failed to connect to {host}.\n")
        except MySQLError as e:
            result_text.insert(tk.END, f"MySQL error: {e}\n")
            messagebox.showerror("MySQL Error", f"Error while fetching slave status: {e}")
        except Exception as e:
            result_text.insert(tk.END, f"Unexpected error: {e}\n")
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")
        finally:
            # Ensure the button is re-enabled and results tab is selected
            root.after(0, lambda: button_check.config(state=tk.NORMAL))
            root.after(0, lambda: tab_control.select(result_tab))  # Switch to Result Tab

    # Run the status check in a separate thread
    threading.Thread(target=fetch_replication_status, daemon=True).start()


# Check replication from main to node and node to node
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
        if not node.get('host') or not node.get('user') or not node.get('password') or not node.get(
                'database') or not node.get('port'):
            messagebox.showerror("Input Error", "Please fill all fields for node server.")
            return
        node_servers.append(node)  # Append the node dictionary to the list

    # Check replication from Main server to Node servers
    fetch_status_for_servers([main_server] + node_servers, connection_type="main_to_node")

    # Check replication from Node server to Node server (for chaining replication)
    fetch_status_for_servers(node_servers, connection_type="node_to_node")


# Add a new node server to the list and display it in the text field
def add_node_server():
    """Add a new node server to the dashboard and display in the result text."""
    node_host = node_host_entry.get()
    node_user = node_user_entry.get()
    node_password = node_password_entry.get()
    node_database = node_database_entry.get()
    node_port = node_port_entry.get()

    # Validate the inputs
    if not node_host or not node_user or not node_password or not node_database or not node_port:
        messagebox.showerror("Input Error", "Please fill in all fields for the node server.")
        return

    # Add the valid node to the list
    node_servers_list.append({
        'host': node_host,
        'user': node_user,
        'password': node_password,
        'database': node_database,
        'port': int(node_port)
    })

    # Update the text field with the added node details
    node_details_text.insert(tk.END, f"Node Server Added:\n")
    node_details_text.insert(tk.END, f"Host: {node_host}\n")
    node_details_text.insert(tk.END, f"User: {node_user}\n")
    node_details_text.insert(tk.END, f"Password: {node_password}\n")
    node_details_text.insert(tk.END, f"Database: {node_database}\n")
    node_details_text.insert(tk.END, f"Port: {node_port}\n")
    node_details_text.insert(tk.END, "-" * 50 + "\n")


# Clear all node server details from the list and text field
def clear_node_servers():
    """Clear all node servers from the list and display."""
    global node_servers_list
    node_servers_list = []  # Clear the list of node servers

    # Confirm action
    confirm = messagebox.askyesno("Confirm Clear", "Are you sure you want to clear all node servers?")
    if confirm:
        # Clear the text field
        node_details_text.delete(1.0, tk.END)
        messagebox.showinfo("Clear Nodes", "All node servers have been cleared.")


# Export results to CSV
def export_results():
    """Export the results in the result text area to a CSV file."""
    results = result_text.get(1.0, tk.END)
    if not results.strip():
        messagebox.showwarning("No Data", "No data available to export.")
        return

    with open("replication_results.csv", "w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Replication Status", "Details"])
        writer.writerows([[line] for line in results.splitlines()])

    messagebox.showinfo("Export Success", "Results successfully exported to 'replication_results.csv'.")


# Setup the GUI
root = tk.Tk()
root.title("MySQL Replication Checker")
root.geometry("1000x700")

# Tabs setup
tab_control = ttk.Notebook(root)
input_tab = ttk.Frame(tab_control)
node_tab = ttk.Frame(tab_control)
result_tab = ttk.Frame(tab_control)
tab_control.add(input_tab, text="Input")
tab_control.add(node_tab, text="Node Servers")
tab_control.add(result_tab, text="Result")
tab_control.pack(expand=1, fill="both")

# Main server input fields
main_server_frame = ttk.LabelFrame(input_tab, text="Main Server Details")
main_server_frame.grid(column=0, row=0, padx=10, pady=10)

ttk.Label(main_server_frame, text="Host:").grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
entry_host = ttk.Entry(main_server_frame, width=25)
entry_host.grid(column=1, row=0, padx=5, pady=5)

ttk.Label(main_server_frame, text="User:").grid(column=0, row=1, sticky=tk.W, padx=5, pady=5)
entry_user = ttk.Entry(main_server_frame, width=25)
entry_user.grid(column=1, row=1, padx=5, pady=5)

ttk.Label(main_server_frame, text="Password:").grid(column=0, row=2, sticky=tk.W, padx=5, pady=5)
entry_password = ttk.Entry(main_server_frame, width=25, show="*")
entry_password.grid(column=1, row=2, padx=5, pady=5)

ttk.Label(main_server_frame, text="Database:").grid(column=0, row=3, sticky=tk.W, padx=5, pady=5)
entry_database = ttk.Entry(main_server_frame, width=25)
entry_database.grid(column=1, row=3, padx=5, pady=5)

ttk.Label(main_server_frame, text="Port:").grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
entry_port = ttk.Entry(main_server_frame, width=25)
entry_port.grid(column=1, row=4, padx=5, pady=5)

# Node server input fields
node_server_frame = ttk.LabelFrame(node_tab, text="Add Node Server")
node_server_frame.grid(column=0, row=0, padx=10, pady=10)

ttk.Label(node_server_frame, text="Node Host:").grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
node_host_entry = ttk.Entry(node_server_frame, width=25)
node_host_entry.grid(column=1, row=0, padx=5, pady=5)

ttk.Label(node_server_frame, text="Node User:").grid(column=0, row=1, sticky=tk.W, padx=5, pady=5)
node_user_entry = ttk.Entry(node_server_frame, width=25)
node_user_entry.grid(column=1, row=1, padx=5, pady=5)

ttk.Label(node_server_frame, text="Node Password:").grid(column=0, row=2, sticky=tk.W, padx=5, pady=5)
node_password_entry = ttk.Entry(node_server_frame, width=25, show="*")
node_password_entry.grid(column=1, row=2, padx=5, pady=5)

ttk.Label(node_server_frame, text="Node Database:").grid(column=0, row=3, sticky=tk.W, padx=5, pady=5)
node_database_entry = ttk.Entry(node_server_frame, width=25)
node_database_entry.grid(column=1, row=3, padx=5, pady=5)

ttk.Label(node_server_frame, text="Node Port:").grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
node_port_entry = ttk.Entry(node_server_frame, width=25)
node_port_entry.grid(column=1, row=4, padx=5, pady=5)

button_add_node = ttk.Button(node_server_frame, text="Add Node Server", command=add_node_server)
button_add_node.grid(column=0, row=5, columnspan=2, padx=5, pady=10)

# Button to clear all nodes
button_clear_node = ttk.Button(node_server_frame, text="Clear All Nodes", command=clear_node_servers)
button_clear_node.grid(column=0, row=6, columnspan=2, padx=5, pady=10)

# Button to export results
button_export = ttk.Button(result_tab, text="Export Results", command=export_results)
button_export.grid(column=0, row=1, padx=10, pady=10)

# Text widget to display added node server details
node_details_text = scrolledtext.ScrolledText(node_tab, width=70, height=20, wrap=tk.WORD)
node_details_text.grid(column=0, row=1, padx=10, pady=10)

# Button to check replication
button_check = ttk.Button(input_tab, text="Check Replication", command=check_replication)
button_check.grid(column=0, row=1, columnspan=2, padx=10, pady=10)

# Text widget to display replication status
result_text = scrolledtext.ScrolledText(result_tab, width=120, height=30, wrap=tk.WORD)
result_text.grid(column=0, row=0, padx=10, pady=10)

root.mainloop()

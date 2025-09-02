#!/usr/bin/env python3
"""
Knesset Research Platform - Desktop Launcher

A simple desktop application launcher for non-technical researchers.
Provides one-click access to the Knesset data analysis platform.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import subprocess
import threading
import os
import sys
import webbrowser
import time
from pathlib import Path

class KnessetLauncher:
    def __init__(self, root):
        self.root = root
        self.root.title("🏛️ Knesset Research Platform")
        self.root.geometry("600x500")
        self.root.resizable(False, False)
        
        # Project paths
        self.project_dir = Path(__file__).parent
        self.venv_python = self.project_dir / ".venv" / "bin" / "python"
        if not self.venv_python.exists():
            self.venv_python = self.project_dir / ".venv" / "Scripts" / "python.exe"  # Windows
        
        self.streamlit_process = None
        self.server_running = False
        
        self.setup_ui()
        self.check_environment()
    
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(
            main_frame, 
            text="🏛️ Knesset Research Platform",
            font=("Arial", 18, "bold")
        )
        title_label.pack(pady=(0, 10))
        
        subtitle_label = ttk.Label(
            main_frame,
            text="Parliamentary Data Analysis Tool",
            font=("Arial", 12)
        )
        subtitle_label.pack(pady=(0, 20))
        
        # Status frame
        status_frame = ttk.LabelFrame(main_frame, text="System Status", padding="10")
        status_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.status_label = ttk.Label(status_frame, text="Checking system...")
        self.status_label.pack()
        
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=(10, 0))
        
        # Buttons frame
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.start_button = ttk.Button(
            buttons_frame,
            text="🚀 Start Research Platform",
            command=self.start_platform,
            style="Accent.TButton"
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.stop_button = ttk.Button(
            buttons_frame,
            text="⏹️ Stop Platform",
            command=self.stop_platform,
            state=tk.DISABLED
        )
        self.stop_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.setup_button = ttk.Button(
            buttons_frame,
            text="⚙️ Setup Environment",
            command=self.run_setup
        )
        self.setup_button.pack(side=tk.LEFT)
        
        # Log frame
        log_frame = ttk.LabelFrame(main_frame, text="Activity Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=12,
            wrap=tk.WORD,
            state=tk.DISABLED
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Help button
        help_button = ttk.Button(
            main_frame,
            text="❓ Help & Documentation",
            command=self.show_help
        )
        help_button.pack(pady=(10, 0))
    
    def log_message(self, message, level="INFO"):
        """Add a message to the log display"""
        self.log_text.config(state=tk.NORMAL)
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {level}: {message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.root.update_idletasks()
    
    def check_environment(self):
        """Check if the environment is properly set up"""
        def check():
            self.progress.start()
            
            # Check if virtual environment exists
            if not self.venv_python.exists():
                self.status_label.config(text="❌ Environment not set up")
                self.log_message("Virtual environment not found. Please run setup first.", "WARNING")
                self.progress.stop()
                return
            
            # Check if required files exist
            required_files = [
                "src/ui/data_refresh.py",
                "requirements.txt"
            ]
            
            for file_path in required_files:
                if not (self.project_dir / file_path).exists():
                    self.status_label.config(text="❌ Missing required files")
                    self.log_message(f"Missing required file: {file_path}", "ERROR")
                    self.progress.stop()
                    return
            
            self.status_label.config(text="✅ Environment ready")
            self.log_message("Environment check completed successfully")
            self.start_button.config(state=tk.NORMAL)
            self.progress.stop()
        
        threading.Thread(target=check, daemon=True).start()
    
    def start_platform(self):
        """Start the Streamlit platform"""
        def start():
            try:
                self.log_message("Starting Knesset Research Platform...")
                self.start_button.config(state=tk.DISABLED)
                self.progress.start()
                
                # Change to project directory
                os.chdir(self.project_dir)
                
                # Start Streamlit
                cmd = [
                    str(self.venv_python),
                    "-m", "streamlit", "run", 
                    "src/ui/data_refresh.py",
                    "--server.port", "8501",
                    "--server.headless", "true",
                    "--browser.gatherUsageStats", "false",
                    "--server.enableCORS", "false"
                ]
                
                self.streamlit_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Wait a moment for server to start
                time.sleep(3)
                
                # Check if process is still running
                if self.streamlit_process.poll() is None:
                    self.server_running = True
                    self.status_label.config(text="🚀 Platform running at http://localhost:8501")
                    self.log_message("Platform started successfully!")
                    self.log_message("Opening web browser...")
                    
                    # Open browser
                    webbrowser.open("http://localhost:8501")
                    
                    self.stop_button.config(state=tk.NORMAL)
                    self.log_message("Click 'Stop Platform' when you're done researching")
                else:
                    # Process failed
                    stdout, stderr = self.streamlit_process.communicate()
                    error_msg = stderr or stdout or "Unknown error"
                    self.log_message(f"Failed to start platform: {error_msg}", "ERROR")
                    self.start_button.config(state=tk.NORMAL)
                    
            except Exception as e:
                self.log_message(f"Error starting platform: {str(e)}", "ERROR")
                self.start_button.config(state=tk.NORMAL)
            finally:
                self.progress.stop()
        
        threading.Thread(target=start, daemon=True).start()
    
    def stop_platform(self):
        """Stop the Streamlit platform"""
        if self.streamlit_process and self.server_running:
            self.log_message("Stopping platform...")
            self.streamlit_process.terminate()
            self.streamlit_process.wait()
            self.server_running = False
            self.status_label.config(text="⏹️ Platform stopped")
            self.log_message("Platform stopped successfully")
            self.start_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
    
    def run_setup(self):
        """Run the setup script"""
        def setup():
            try:
                self.log_message("Running environment setup...")
                self.setup_button.config(state=tk.DISABLED)
                self.progress.start()
                
                # Run setup script
                setup_script = self.project_dir / "setup_for_researcher.py"
                if setup_script.exists():
                    result = subprocess.run([
                        sys.executable, str(setup_script)
                    ], capture_output=True, text=True, cwd=self.project_dir)
                    
                    if result.returncode == 0:
                        self.log_message("Setup completed successfully!")
                        self.check_environment()
                    else:
                        self.log_message(f"Setup failed: {result.stderr}", "ERROR")
                else:
                    self.log_message("Setup script not found. Please check installation.", "ERROR")
                    
            except Exception as e:
                self.log_message(f"Setup error: {str(e)}", "ERROR")
            finally:
                self.setup_button.config(state=tk.NORMAL)
                self.progress.stop()
        
        threading.Thread(target=setup, daemon=True).start()
    
    def show_help(self):
        """Show help dialog"""
        help_text = """
🏛️ Knesset Research Platform - Quick Guide

Getting Started:
1. Click 'Setup Environment' (first time only)
2. Click 'Start Research Platform'  
3. Your web browser will open automatically
4. Use the web interface to explore data
5. Click 'Stop Platform' when done

Features Available:
• Browse parliamentary data tables
• Run predefined analytical queries  
• Create interactive visualizations
• Export data to Excel/CSV
• Analyze bills, queries, and agendas

Need Help?
• Check the Activity Log for status updates
• Ensure you have internet connection for data
• Contact technical support if issues persist

Web Interface URL: http://localhost:8501
        """
        
        messagebox.showinfo("Help - Knesset Research Platform", help_text)
    
    def on_closing(self):
        """Handle window closing"""
        if self.server_running:
            if messagebox.askokcancel("Quit", "Platform is still running. Stop it before closing?"):
                self.stop_platform()
                time.sleep(1)  # Give it time to stop
                self.root.destroy()
        else:
            self.root.destroy()

def main():
    root = tk.Tk()
    
    # Set up theme
    style = ttk.Style()
    style.theme_use('clam')
    
    app = KnessetLauncher(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    # Center the window
    root.eval('tk::PlaceWindow . center')
    
    root.mainloop()

if __name__ == "__main__":
    main()
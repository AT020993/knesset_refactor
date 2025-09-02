#!/usr/bin/env python3
"""
Knesset Research Platform - Simple Command Line Launcher

A simple script to start the platform with proper error handling.
Alternative to the GUI launcher for command line users.
"""

import os
import sys
import subprocess
import webbrowser
import time
from pathlib import Path

def print_banner():
    """Print startup banner"""
    print("\n" + "="*60)
    print("🏛️  KNESSET RESEARCH PLATFORM")
    print("    Parliamentary Data Analysis Tool")
    print("="*60)

def check_environment():
    """Check if environment is properly set up"""
    project_dir = Path(__file__).parent
    
    # Check for virtual environment
    venv_python = project_dir / ".venv" / "bin" / "python"
    if not venv_python.exists():
        venv_python = project_dir / ".venv" / "Scripts" / "python.exe"  # Windows
    
    if not venv_python.exists():
        print("❌ ERROR: Virtual environment not found!")
        print("📋 Please run the setup first:")
        print("   python setup_for_researcher.py")
        return None, project_dir
    
    # Check for main application
    main_app = project_dir / "src" / "ui" / "data_refresh.py"
    if not main_app.exists():
        print("❌ ERROR: Main application not found!")
        print("📋 Please check your installation.")
        return None, project_dir
    
    print("✅ Environment check passed")
    return venv_python, project_dir

def start_streamlit(venv_python, project_dir):
    """Start the Streamlit server"""
    try:
        print("🚀 Starting Knesset Research Platform...")
        print("   Please wait while the server starts up...")
        
        # Change to project directory
        os.chdir(project_dir)
        
        # Prepare Streamlit command
        cmd = [
            str(venv_python),
            "-m", "streamlit", "run",
            "src/ui/data_refresh.py",
            "--server.port", "8501",
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false"
        ]
        
        print("🔄 Starting server...")
        
        # Start Streamlit process
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for server to start
        print("⏳ Waiting for server startup...")
        time.sleep(5)
        
        # Check if process is running
        if process.poll() is None:
            print("✅ Server started successfully!")
            print("🌐 Platform available at: http://localhost:8501")
            print("📖 Opening web browser...")
            
            # Open browser
            webbrowser.open("http://localhost:8501")
            
            print("\n" + "="*60)
            print("🎉 PLATFORM IS RUNNING!")
            print("="*60)
            print("📋 Instructions:")
            print("   • Your web browser should open automatically")
            print("   • If not, go to: http://localhost:8501")
            print("   • Use the web interface to explore data")
            print("   • Press Ctrl+C here to stop the platform")
            print("="*60)
            
            # Keep process running
            try:
                process.wait()
            except KeyboardInterrupt:
                print("\n🛑 Stopping platform...")
                process.terminate()
                process.wait()
                print("✅ Platform stopped")
                
        else:
            # Process failed to start
            stdout, stderr = process.communicate()
            print("❌ Failed to start platform")
            print("📋 Error details:")
            if stderr:
                print(f"   {stderr}")
            if stdout:
                print(f"   {stdout}")
            return False
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    
    return True

def show_help():
    """Show help information"""
    help_text = """
🏛️ Knesset Research Platform - Command Line Launcher

Usage:
    python launch_knesset.py          # Start the platform
    python launch_knesset.py --help   # Show this help

First Time Setup:
    python setup_for_researcher.py    # Run this first

What This Does:
    1. Checks your environment is set up correctly
    2. Starts the research platform web server
    3. Opens your web browser automatically
    4. Provides a user-friendly interface for data analysis

Web Interface Features:
    • Browse parliamentary data tables
    • Run predefined analytical queries
    • Create interactive visualizations  
    • Export data to Excel/CSV formats
    • Access help and documentation

Stopping the Platform:
    • Press Ctrl+C in this terminal window
    • Or close this terminal window

Troubleshooting:
    • If setup errors occur, run: python setup_for_researcher.py
    • Ensure you have Python 3.8+ installed
    • Check that you have internet connection for data fetching
    
Alternative Launcher:
    • For a graphical interface: python researcher_launcher.py
    
URL: http://localhost:8501
"""
    print(help_text)

def main():
    """Main launcher function"""
    # Check for help flag
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', 'help']:
        show_help()
        return
    
    print_banner()
    
    # Check environment
    venv_python, project_dir = check_environment()
    if not venv_python:
        print("\n❌ Cannot start platform - environment not ready")
        print("🔧 Run setup first: python setup_for_researcher.py")
        input("\nPress Enter to exit...")
        return
    
    # Start the platform
    success = start_streamlit(venv_python, project_dir)
    
    if not success:
        print("\n❌ Platform failed to start")
        print("🔧 Try running setup again: python setup_for_researcher.py")
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Knesset Research Platform - One-Time Setup Script

Automatically sets up the environment for researchers with no coding experience.
This script handles all technical setup requirements.
"""

import os
import sys
import subprocess
import platform
import venv
from pathlib import Path
import urllib.request
import json

class KnessetSetup:
    def __init__(self):
        self.project_dir = Path(__file__).parent
        self.venv_dir = self.project_dir / ".venv"
        self.system = platform.system()
        
        # Platform-specific paths
        if self.system == "Windows":
            self.venv_python = self.venv_dir / "Scripts" / "python.exe"
            self.venv_pip = self.venv_dir / "Scripts" / "pip.exe"
            self.activation_script = self.venv_dir / "Scripts" / "activate.bat"
        else:
            self.venv_python = self.venv_dir / "bin" / "python"
            self.venv_pip = self.venv_dir / "bin" / "pip"
            self.activation_script = self.venv_dir / "bin" / "activate"
    
    def print_step(self, step, message):
        """Print formatted step message"""
        print(f"\n{'='*60}")
        print(f"STEP {step}: {message}")
        print('='*60)
    
    def check_python_version(self):
        """Verify Python version is compatible"""
        self.print_step(1, "Checking Python version")
        
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 8):
            print(f"❌ ERROR: Python {version.major}.{version.minor} detected")
            print("❌ Minimum required: Python 3.8+")
            print("📥 Please install Python 3.8 or higher from python.org")
            return False
        
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible!")
        return True
    
    def create_virtual_environment(self):
        """Create virtual environment if it doesn't exist"""
        self.print_step(2, "Setting up virtual environment")
        
        if self.venv_dir.exists():
            print("📁 Virtual environment already exists")
            return True
        
        try:
            print("🔨 Creating virtual environment...")
            venv.create(self.venv_dir, with_pip=True)
            print("✅ Virtual environment created successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to create virtual environment: {e}")
            return False
    
    def install_dependencies(self):
        """Install required Python packages"""
        self.print_step(3, "Installing dependencies")
        
        requirements_file = self.project_dir / "requirements.txt"
        if not requirements_file.exists():
            print("❌ requirements.txt not found")
            return False
        
        try:
            print("📦 Installing Python packages...")
            print("   (This may take a few minutes)")
            
            # Upgrade pip first
            subprocess.run([
                str(self.venv_python), "-m", "pip", "install", "--upgrade", "pip"
            ], check=True, cwd=self.project_dir)
            
            # Install requirements
            result = subprocess.run([
                str(self.venv_pip), "install", "-r", "requirements.txt"
            ], check=True, cwd=self.project_dir, capture_output=True, text=True)
            
            print("✅ All dependencies installed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    def verify_installation(self):
        """Verify that key components are working"""
        self.print_step(4, "Verifying installation")
        
        try:
            # Test basic imports
            print("🧪 Testing core libraries...")
            test_script = '''
import streamlit
import duckdb  
import pandas
import plotly
print("All core libraries imported successfully")
'''
            
            result = subprocess.run([
                str(self.venv_python), "-c", test_script
            ], check=True, capture_output=True, text=True, cwd=self.project_dir)
            
            print("✅ Core libraries working correctly")
            
            # Check if main application file exists
            main_app = self.project_dir / "src" / "ui" / "data_refresh.py"
            if main_app.exists():
                print("✅ Main application found")
            else:
                print("❌ Main application not found")
                return False
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Verification failed: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    def download_sample_data(self):
        """Download minimal sample data for testing"""
        self.print_step(5, "Setting up sample data")
        
        try:
            print("📊 Downloading sample data...")
            print("   (This helps verify the system works)")
            
            # Run the data fetch for a small table
            result = subprocess.run([
                str(self.venv_python), "-m", "backend.fetch_table", 
                "--table", "KNS_Person"
            ], check=True, capture_output=True, text=True, 
              cwd=self.project_dir, env={**os.environ, "PYTHONPATH": str(self.project_dir / "src")})
            
            print("✅ Sample data downloaded successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            print("⚠️  Sample data download failed (this is optional)")
            print("   The platform will still work, but may need data refresh first")
            return True  # Not critical for setup
    
    def create_desktop_shortcut(self):
        """Create desktop shortcut for easy access"""
        self.print_step(6, "Creating desktop shortcut")
        
        try:
            desktop = Path.home() / "Desktop"
            if not desktop.exists():
                print("📁 Desktop folder not found - skipping shortcut")
                return True
            
            launcher_script = self.project_dir / "researcher_launcher.py"
            if not launcher_script.exists():
                print("⚠️  Launcher script not found - skipping shortcut")
                return True
            
            if self.system == "Windows":
                self._create_windows_shortcut(desktop, launcher_script)
            else:
                self._create_unix_shortcut(desktop, launcher_script)
            
            print("✅ Desktop shortcut created")
            return True
            
        except Exception as e:
            print(f"⚠️  Could not create desktop shortcut: {e}")
            print("   You can still run the launcher manually")
            return True  # Not critical
    
    def _create_windows_shortcut(self, desktop, launcher_script):
        """Create Windows shortcut"""
        import winshell
        from win32com.client import Dispatch
        
        shortcut_path = desktop / "Knesset Research Platform.lnk"
        shell = Dispatch('WScript.Shell')
        shortcut = shell.CreateShortCut(str(shortcut_path))
        shortcut.Targetpath = str(self.venv_python)
        shortcut.Arguments = f'"{launcher_script}"'
        shortcut.WorkingDirectory = str(self.project_dir)
        shortcut.IconLocation = str(self.venv_python)
        shortcut.save()
    
    def _create_unix_shortcut(self, desktop, launcher_script):
        """Create Unix shell script shortcut"""
        if self.system == "Darwin":  # macOS
            # Create a simple shell script for macOS
            shortcut_content = f'''#!/bin/bash
cd "{self.project_dir}"
python3 launch_knesset.py
'''
            shortcut_path = desktop / "Start Knesset Research.command"
            with open(shortcut_path, 'w') as f:
                f.write(shortcut_content)
            # Make executable
            os.chmod(shortcut_path, 0o755)
        else:
            # Linux desktop file
            shortcut_content = f"""[Desktop Entry]
Name=Knesset Research Platform
Comment=Parliamentary Data Analysis Tool
Exec=bash -c 'cd "{self.project_dir}" && python3 launch_knesset.py'
Path={self.project_dir}
Icon=applications-science
Terminal=true
Type=Application
Categories=Office;Education;
"""
            shortcut_path = desktop / "knesset-research.desktop"
            with open(shortcut_path, 'w') as f:
                f.write(shortcut_content)
            # Make executable
            os.chmod(shortcut_path, 0o755)
    
    def print_completion_message(self):
        """Print final setup completion message"""
        print("\n" + "="*60)
        print("🎉 SETUP COMPLETED SUCCESSFULLY!")
        print("="*60)
        print()
        print("Your Knesset Research Platform is ready to use!")
        print()
        print("📋 What's been set up:")
        print("   ✅ Python virtual environment")
        print("   ✅ All required software packages")
        print("   ✅ Sample data for testing")
        print("   ✅ Desktop launcher (if possible)")
        print()
        print("🚀 To start researching:")
        print("   1. Double-click 'Knesset Research Platform' on your desktop")
        print("   2. OR run: python researcher_launcher.py")
        print("   3. Click 'Start Research Platform' in the launcher")
        print("   4. Your web browser will open automatically")
        print()
        print("📖 The web interface provides:")
        print("   • Data tables and visualizations")
        print("   • Predefined analytical queries")
        print("   • Export capabilities for Excel/CSV")
        print("   • Help documentation")
        print()
        print("❓ Need help? Check the built-in help or contact technical support")
        print("="*60)

def main():
    print("🏛️ Knesset Research Platform - Automated Setup")
    print("Setting up your research environment...")
    
    setup = KnessetSetup()
    
    # Run setup steps
    steps = [
        setup.check_python_version,
        setup.create_virtual_environment,
        setup.install_dependencies,
        setup.verify_installation,
        setup.download_sample_data,
        setup.create_desktop_shortcut
    ]
    
    for step_func in steps:
        if not step_func():
            print("\n❌ Setup failed. Please contact technical support.")
            input("\nPress Enter to exit...")
            return False
    
    # Success!
    setup.print_completion_message()
    input("\nPress Enter to close this window...")
    return True

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Knesset Research Platform - Simple Web-Based Launcher

A fallback launcher that uses the web browser instead of tkinter GUI.
Works on all systems including macOS with Homebrew Python.
"""

import subprocess
import webbrowser
import time
import sys
from pathlib import Path
import tempfile
import os

class SimpleWebLauncher:
    def __init__(self):
        self.project_dir = Path(__file__).parent
        self.venv_python = self.project_dir / ".venv" / "bin" / "python"
        if not self.venv_python.exists():
            self.venv_python = self.project_dir / ".venv" / "Scripts" / "python.exe"  # Windows
    
    def create_launcher_html(self):
        """Create a simple HTML launcher interface"""
        html_content = '''
<!DOCTYPE html>
<html>
<head>
    <title>🏛️ Knesset Research Platform Launcher</title>
    <meta charset="utf-8">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 40px 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            min-height: 100vh;
        }
        .container {
            background: rgba(255, 255, 255, 0.95);
            color: #333;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        }
        h1 {
            color: #2c3e50;
            text-align: center;
            margin-bottom: 10px;
            font-size: 2.5em;
        }
        .subtitle {
            text-align: center;
            color: #666;
            margin-bottom: 40px;
            font-size: 1.2em;
        }
        .status {
            background: #f8f9fa;
            border: 2px solid #e9ecef;
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
        }
        .button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 15px 30px;
            font-size: 1.1em;
            border-radius: 10px;
            cursor: pointer;
            margin: 10px;
            text-decoration: none;
            display: inline-block;
            transition: all 0.3s ease;
        }
        .button:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        .success { color: #28a745; }
        .error { color: #dc3545; }
        .warning { color: #ffc107; }
        .instructions {
            background: #e8f4fd;
            border-left: 5px solid #007bff;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }
        .features {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        .feature {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid #007bff;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏛️ Knesset Research Platform</h1>
        <p class="subtitle">Parliamentary Data Analysis Tool</p>
        
        <div class="status" id="status">
            <h3>🔍 System Status</h3>
            <div id="status-content">Checking environment...</div>
        </div>
        
        <div style="text-align: center; margin: 30px 0;">
            <button class="button" onclick="startPlatform()">🚀 Start Research Platform</button>
            <button class="button" onclick="showHelp()">❓ Help & Instructions</button>
        </div>
        
        <div class="instructions" id="instructions" style="display: none;">
            <h3>📋 Quick Instructions</h3>
            <ol>
                <li><strong>Click "Start Research Platform"</strong> above</li>
                <li><strong>Wait</strong> for the system to start up (30-60 seconds)</li>
                <li><strong>New browser tab</strong> will open automatically with the research interface</li>
                <li><strong>Use the web interface</strong> to explore parliamentary data</li>
                <li><strong>Close this tab</strong> when you're done researching</li>
            </ol>
        </div>
        
        <div class="features">
            <div class="feature">
                <h4>📊 Data Analysis</h4>
                <p>Browse parliamentary tables, run predefined queries, and explore legislative data.</p>
            </div>
            <div class="feature">
                <h4>📈 Visualizations</h4>
                <p>Create interactive charts and graphs to understand parliamentary patterns.</p>
            </div>
            <div class="feature">
                <h4>📥 Data Export</h4>
                <p>Download results to Excel or CSV for further analysis.</p>
            </div>
            <div class="feature">
                <h4>🔍 Advanced Search</h4>
                <p>Filter data by Knesset terms, factions, dates, and other criteria.</p>
            </div>
        </div>
        
        <div style="text-align: center; margin-top: 40px; color: #666;">
            <p><strong>URL:</strong> <code>http://localhost:8501</code></p>
            <p><em>This launcher ensures easy access without command line knowledge</em></p>
        </div>
    </div>

    <script>
        // Check system status
        function checkStatus() {
            const statusContent = document.getElementById('status-content');
            statusContent.innerHTML = '<span class="success">✅ Launcher ready</span><br>Click "Start Research Platform" to begin';
        }
        
        function startPlatform() {
            const statusContent = document.getElementById('status-content');
            statusContent.innerHTML = '<span class="warning">🚀 Starting platform...</span><br>Please wait 30-60 seconds for the server to start up';
            
            // Make request to start the platform
            fetch('/start', {method: 'POST'})
                .then(response => response.text())
                .then(data => {
                    statusContent.innerHTML = '<span class="success">✅ Platform started!</span><br>Opening research interface...';
                    // Open the Streamlit app
                    setTimeout(() => {
                        window.open('http://localhost:8501', '_blank');
                    }, 2000);
                })
                .catch(error => {
                    statusContent.innerHTML = '<span class="error">❌ Failed to start platform</span><br>Try the command line launcher instead: python launch_knesset.py';
                });
        }
        
        function showHelp() {
            const instructions = document.getElementById('instructions');
            instructions.style.display = instructions.style.display === 'none' ? 'block' : 'none';
        }
        
        // Initialize
        window.onload = checkStatus;
    </script>
</body>
</html>
'''
        
        # Save to temp file
        temp_dir = tempfile.gettempdir()
        html_file = Path(temp_dir) / "knesset_launcher.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return html_file
    
    def launch(self):
        """Launch the web-based interface"""
        print("🏛️ Knesset Research Platform - Simple Launcher")
        print("="*50)
        
        # Check environment
        if not self.venv_python.exists():
            print("❌ Virtual environment not found!")
            print("📋 Please run setup first: python setup_for_researcher.py")
            input("\nPress Enter to exit...")
            return
        
        # Create and open HTML launcher
        html_file = self.create_launcher_html()
        print(f"🌐 Opening launcher interface...")
        webbrowser.open(f"file://{html_file}")
        
        print("\n📋 Instructions:")
        print("   1. Web browser should open with launcher interface")
        print("   2. Click 'Start Research Platform' in the browser")
        print("   3. Wait for research interface to load")
        print("\n🔧 Alternative: Run 'python launch_knesset.py' for command line version")
        print("\nPress Enter to close this window...")
        input()

def main():
    launcher = SimpleWebLauncher()
    launcher.launch()

if __name__ == "__main__":
    main()
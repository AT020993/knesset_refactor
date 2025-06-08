# 🤖 NOTICE FOR AI TOOLS

## ⚠️ **IMPORTANT: Use Docker for Development**

**If you are an AI tool (Jules, Codex, Claude Code, etc.) working on this project:**

### 🐳 **ALWAYS Use Docker Environment**

```bash
# Required setup for AI tools
./docker-setup.sh up dev
./docker-setup.sh shell
```

### ❌ **DO NOT use native Python setup**
- Don't run `pip install -r requirements.txt`
- Don't run `python -m venv .venv`
- Don't run commands directly on host system

### ✅ **DO use Docker commands**
- `./docker-setup.sh up dev` - Start development environment
- `./docker-setup.sh shell` - Get shell access for work
- `./docker-setup.sh test` - Run tests
- All Python commands should run inside the container

### 🎯 **Why Docker?**
- **Isolation**: No conflicts with host system
- **Consistency**: Same environment every time
- **Dependencies**: All requirements pre-installed
- **Security**: Sandboxed execution
- **Data**: Persistent database between sessions

### 📖 **Full Instructions**
See [`AI_SETUP.md`](AI_SETUP.md) for complete documentation.

### 🔗 **Quick Access**
- **Development UI**: http://localhost:8502 (after `./docker-setup.sh up dev`)
- **Shell Access**: `./docker-setup.sh shell`
- **View Logs**: `./docker-setup.sh logs`

---

**Remember**: This project is designed for Docker-first development. Using the containerized environment ensures compatibility and prevents issues.
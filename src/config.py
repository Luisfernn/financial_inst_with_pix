# src/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Define o caminho base
base_dir = Path(__file__).resolve().parent.parent

# Tenta carregar o .env.local se ele existir (uso local)
# Se não existir, assume que as variáveis virão do ambiente (Docker)
local_env = base_dir / ".env.local"
if local_env.exists():
    load_dotenv(local_env)
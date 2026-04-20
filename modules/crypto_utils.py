"""AES-256-GCM encryption/decryption for private keys."""

import os
import re
import sys
import getpass
from pathlib import Path

from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from modules import logger

MAGIC     = b"BONUS9KEYS000001"   # 16 bytes
SALT_LEN  = 32
NONCE_LEN = 12
ITER      = 200_000


def _derive_key(password: bytes, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                     salt=salt, iterations=ITER)
    return kdf.derive(password)


def encrypt_keys(keys_path: Path, enc_path: Path) -> None:
    password = getpass.getpass("Придумайте пароль для шифрования: ").encode()
    confirm  = getpass.getpass("Повторите пароль: ").encode()
    if password != confirm:
        logger.error("Пароли не совпадают")
        sys.exit(1)
    plaintext = keys_path.read_bytes()
    salt  = os.urandom(SALT_LEN)
    nonce = os.urandom(NONCE_LEN)
    key   = _derive_key(password, salt)
    ct    = AESGCM(key).encrypt(nonce, plaintext, None)
    enc_path.write_bytes(MAGIC + salt + nonce + ct)
    logger.success(f"Ключи зашифрованы → {enc_path}")


def decrypt_keys(enc_path: Path) -> str:
    data = enc_path.read_bytes()
    if data[:16] != MAGIC:
        logger.error("Неверный формат keys.enc")
        sys.exit(1)
    salt  = data[16:16+SALT_LEN]
    nonce = data[16+SALT_LEN:16+SALT_LEN+NONCE_LEN]
    ct    = data[16+SALT_LEN+NONCE_LEN:]
    for attempt in range(3):
        password = getpass.getpass(f"Пароль от keys.enc (попытка {attempt+1}/3): ").encode()
        try:
            key = _derive_key(password, salt)
            return AESGCM(key).decrypt(nonce, ct, None).decode()
        except Exception:
            logger.error("Неверный пароль")
    logger.error("Превышено количество попыток")
    sys.exit(1)


def load_keys_plaintext(project_root: Path) -> list[str]:
    """Return list of raw private key strings (with 0x prefix)."""
    enc_path  = project_root / "keys.enc"
    keys_path = project_root / "keys.txt"

    if enc_path.exists():
        raw = decrypt_keys(enc_path)
    elif keys_path.exists():
        raw = keys_path.read_text(encoding="utf-8")
        answer = input("Зашифровать keys.txt? (y/N): ").strip().lower()
        if answer == "y":
            encrypt_keys(keys_path, enc_path)
    else:
        logger.error("Не найден keys.txt или keys.enc")
        sys.exit(1)

    hex_re = re.compile(r"^(0x)?[0-9a-fA-F]{64}$")
    keys = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not hex_re.match(line):
            logger.warning(f"Пропускаю невалидный ключ: {line[:10]}...")
            continue
        keys.append(line if line.startswith("0x") else "0x" + line)
    return keys

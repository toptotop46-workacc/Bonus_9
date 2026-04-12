"""
SoundChains — Direct mintMyMusic NFT mint (task 2).
Contract : 0x2B388C9bF1883095DCf949EF5d0ACfA7581289a0
Function : mintMyMusic(string,string,string,string,string)  selector 0x88c56172
"""

import time
import random
import string
from urllib.parse import quote
from web3 import Web3
from eth_account import Account
from modules import logger, db, web3_utils
from modules.portal_api import require_account_status

import eth_abi

MINT_CONTRACT = "0x2B388C9bF1883095DCf949EF5d0ACfA7581289a0"
# mintMyMusic(string,string,string,string,string)
MINT_SELECTOR = bytes.fromhex("88c56172")

AUDIO_HOST    = "https://tempfile.aiquickdraw.com/r"
METADATA_BASE = "https://soundchains.org/api/metadata/nft"
IMAGE_URL     = "https://soundchains.org/soundchainlinkimage.png"

ADJECTIVES = [
    "Cosmic", "Digital", "Neon", "Shadow", "Crystal", "Electric",
    "Mystic", "Quantum", "Solar", "Lunar", "Cyber", "Phantom",
    "Stellar", "Vortex", "Frozen", "Velvet", "Golden", "Silent",
    "Dark", "Bright", "Deep", "Wild", "Broken", "Hollow",
]

NOUNS = [
    "Wave", "Dream", "Odyssey", "Journey", "Storm", "Horizon",
    "Vision", "Realm", "Surge", "Flow", "Beat", "Drift",
    "Current", "Signal", "Frequency", "Motion", "Rain", "Fire",
    "Night", "Void", "Sky", "River", "Forest", "City",
]

STYLES = [
    "cyberpunk", "lo-fi", "electronic", "ambient", "synthwave",
    "chillout", "downtempo", "future bass", "deep house", "trap",
    "vaporwave", "jazz fusion", "neo soul", "melodic techno",
    "drum and bass", "indie pop", "dark ambient", "post-rock",
]

# ── Data generators ───────────────────────────────────────────────────────────

def random_song_title() -> str:
    patterns = [
        lambda: f"{random.choice(ADJECTIVES)} {random.choice(NOUNS)}",
        lambda: f"The {random.choice(ADJECTIVES)} {random.choice(NOUNS)}",
        lambda: f"{random.choice(NOUNS)} of {random.choice(ADJECTIVES).lower()} {random.choice(NOUNS).lower()}",
        lambda: f"{random.choice(ADJECTIVES)} {random.choice(ADJECTIVES).lower()} {random.choice(NOUNS).lower()}",
    ]
    return random.choice(patterns)()


def random_style() -> str:
    return random.choice(STYLES)


def _random_hex(length: int = 32) -> str:
    """Random lowercase hex string — simulates a real file hash."""
    return "".join(random.choices("0123456789abcdef", k=length))


def _build_music_id(title: str) -> str:
    """sc_{title_lower} 1.mp3_{timestamp_ms}  — matches real site format."""
    ts   = int(time.time() * 1000) + random.randint(-3000, 3000)
    slug = title.lower()
    return f"sc_{slug} 1.mp3_{ts}"


def _build_audio_url() -> str:
    return f"{AUDIO_HOST}/{_random_hex(32)}.mp3"


def _build_metadata_uri(title: str, style: str,
                         audio_url: str, music_id: str) -> str:
    params = [
        ("title",   title),
        ("style",   style),
        ("audio",   audio_url),
        ("musicId", music_id),
        ("image",   IMAGE_URL),
    ]
    qs = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
    return f"{METADATA_BASE}?{qs}"


# ── On-chain mint ─────────────────────────────────────────────────────────────

def mint_my_music(
    private_key: str,
    w3: Web3,
    title: str,
    style: str,
    gas_limit_multiplier: float = 1.3,
) -> str:
    """Encode and send mintMyMusic tx. Returns tx hash."""
    music_id  = _build_music_id(title)
    audio_url = _build_audio_url()
    meta_uri  = _build_metadata_uri(title, style, audio_url, music_id)

    logger.debug(
        f"[SoundChains] mint meta id={music_id} title={title!r} style={style!r} "
        f"audio={audio_url[:48]}… meta={meta_uri[:64]}…"
    )

    calldata = MINT_SELECTOR + eth_abi.encode(
        ["string", "string", "string", "string", "string"],
        [music_id, title, style, audio_url, meta_uri],
    )

    return web3_utils.build_and_send_tx(
        w3=w3,
        private_key=private_key,
        to=MINT_CONTRACT,
        data=calldata,
        value=0,
        gas_limit_multiplier=gas_limit_multiplier,
    )


# ── Main flow ─────────────────────────────────────────────────────────────────

def run_soundchains(
    private_key: str,
    w3: Web3,
    proxy: str | None = None,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    """Direct mintMyMusic call — no API login required."""
    account     = Account.from_key(private_key)
    eoa_address = account.address

    st = require_account_status(eoa_address, proxy, proxy_pool=proxy_pool)
    if st.get("soundchains_done"):
        logger.info(f"[SoundChains] {eoa_address} портал OK, минт не нужен")
        db.upsert_account(eoa_address, soundchains_done=True)
        return True

    title = random_song_title()
    style = random_style()
    logger.info(f"[SoundChains] mint «{title}» | {style}")

    # Small human-like pause before sending tx
    time.sleep(random.uniform(2, 7))

    try:
        tx_hash = mint_my_music(private_key, w3, title, style)
        db.mark_soundchains_done(eoa_address, tx_hash)
        logger.success(
            f"[SoundChains] OK {eoa_address} tx {tx_hash[:10]}…{tx_hash[-6:]}"
        )
        return True
    except Exception as e:
        logger.error(f"[SoundChains] mint: {e}")
        return False

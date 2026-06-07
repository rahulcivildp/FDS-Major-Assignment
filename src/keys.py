from pathlib import Path
from cryptography.hazmat.primitives import serialization
from crypto_utils import generate_keys

KEY_DIR = Path("keys")
KEY_DIR.mkdir(exist_ok=True)


def load_or_create_keys(node_id):

    private_file = KEY_DIR / f"node_{node_id}_private.pem"
    public_file = KEY_DIR / f"node_{node_id}_public.pem"

    if private_file.exists() and public_file.exists():

        with open(private_file, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=None
            )

        with open(public_file, "rb") as f:
            public_key = serialization.load_pem_public_key(
                f.read()
            )

        return private_key, public_key

    private_key, public_key = generate_keys()

    with open(private_file, "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        )

    with open(public_file, "wb") as f:
        f.write(
            public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        )

    return private_key, public_key


def load_all_public_keys():

    keys = {}

    for node_id in range(0, 6):

        public_file = KEY_DIR / f"node_{node_id}_public.pem"

        if public_file.exists():

            with open(public_file, "rb") as f:
                keys[node_id] = serialization.load_pem_public_key(
                    f.read()
                )

    return keys
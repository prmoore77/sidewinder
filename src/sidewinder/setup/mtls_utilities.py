import os
import cryptography
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.x509.oid import NameOID
from datetime import datetime, timedelta
from pathlib import Path
import click

# Constants
CERTIFICATE_VERSION: int = (3 - 1)

TLS_DIR = Path("tls").resolve()


@click.command()
@click.option(
    "--ca-common-name",
    type=str,
    default="Flight Ibis Demo CA",
    required=True,
    help="The common name to create the Certificate Authority for."
)
@click.option(
    "--ca-cert-file",
    type=str,
    default=(TLS_DIR / "ca.crt").as_posix(),
    required=True,
    help="The CA certificate file to create."
)
@click.option(
    "--ca-key-file",
    type=str,
    default=(TLS_DIR / "ca.key").as_posix(),
    required=True,
    help="The CA key file to create."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the CA cert/key if they exist?"
)
@click.option(
    "--password",
    type=str,
    required=False,
    help="An optional password to encrypt the CA certificate keypair"
)
def create_ca_keypair(
        ca_common_name: str,
        ca_cert_file: str,
        ca_key_file: str,
        overwrite: bool,
        password: str
):
    ca_cert_file_path = Path(ca_cert_file)
    ca_key_file_path = Path(ca_key_file)

    if ca_cert_file_path.exists() or ca_key_file_path.exists():
        if not overwrite:
            raise RuntimeError(f"The CA Cert file(s): '{ca_cert_file_path.as_posix()}' or '{ca_key_file_path.as_posix()}' - exist - and overwrite is False, aborting.")
        else:
            ca_cert_file_path.unlink(missing_ok=True)
            ca_key_file_path.unlink(missing_ok=True)

    # Generate a new key pair for the CA
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    # Create a self-signed CA certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, ca_common_name)
    ])

    ca_cert = x509.CertificateBuilder().subject_name(
        name=subject
    ).issuer_name(
        name=issuer
    ).public_key(
        key=private_key.public_key()
    ).serial_number(
        number=x509.random_serial_number()
    ).not_valid_before(
        time=datetime.utcnow()
    ).not_valid_after(
        time=datetime.utcnow() + timedelta(days=365)
    ).add_extension(
        extval=x509.BasicConstraints(ca=True, path_length=None), critical=True
    ).sign(
        private_key=private_key, algorithm=SHA256(), backend=default_backend()
    )

    # Write the CA certificate and key to disk
    with open(ca_cert_file_path, "wb") as f:
        f.write(ca_cert.public_bytes(encoding=serialization.Encoding.PEM))

    if password:
        encryption_algorithm = serialization.BestAvailableEncryption(password.encode())
    else:
        encryption_algorithm = serialization.NoEncryption()
    with open(ca_key_file_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=encryption_algorithm
        ))

    print(f"Successfully created Certificate Authority (CA) keypair for Common Name (CN): '{ca_common_name}'")
    print(f"CA Cert file: {ca_cert_file_path.as_posix()}")
    print(f"CA Key file: {ca_key_file_path.as_posix()}")


@click.command()
@click.option(
    "--client-common-name",
    type=str,
    default="Flight Ibis Demo Client",
    required=True,
    help="The common name to create the Client certificate for."
)
@click.option(
    "--ca-cert-file",
    type=str,
    default=(TLS_DIR / "ca.crt").as_posix(),
    required=True,
    help="The CA certificate file used to sign the client certificate - is MUST exist."
)
@click.option(
    "--ca-key-file",
    type=str,
    default=(TLS_DIR / "ca.key").as_posix(),
    required=True,
    help="The CA key file used to sign the client certificate - is MUST exist."
)
@click.option(
    "--client-cert-file",
    type=str,
    default=(TLS_DIR / "client.crt").as_posix(),
    required=True,
    help="The Client certificate file to create."
)
@click.option(
    "--client-key-file",
    type=str,
    default=(TLS_DIR / "client.key").as_posix(),
    required=True,
    help="The Client key file to create."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the client cert/key if they exist?"
)
@click.option(
    "--ca-password",
    type=str,
    required=False,
    help="The CA private key password (if it is encrypted)"
)
@click.option(
    "--password",
    type=str,
    required=False,
    help="An optional password to encrypt the certificate keypair"
)
def create_client_keypair(
        client_common_name: str,
        ca_cert_file: str,
        ca_key_file: str,
        client_cert_file: str,
        client_key_file: str,
        overwrite: bool,
        ca_password: str,
        password: str
):
    ca_cert_file_path = Path(ca_cert_file)
    ca_key_file_path = Path(ca_key_file)

    if not ca_cert_file_path.exists():
        raise RuntimeError(f"The CA Cert file: '{ca_cert_file_path.as_posix()}' does not exist, aborting")

    if not ca_key_file_path.exists():
        raise RuntimeError(f"The CA Key file: '{ca_key_file_path.as_posix()}' does not exist, aborting")

    client_cert_file_path = Path(client_cert_file)
    client_key_file_path = Path(client_key_file)

    if client_cert_file_path.exists() or client_key_file_path.exists():
        if not overwrite:
            raise RuntimeError(f"The Client Cert file(s): '{client_cert_file_path.as_posix()}' or '{client_key_file_path.as_posix()}' - exist - and overwrite is False, aborting.")
        else:
            client_cert_file_path.unlink(missing_ok=True)
            client_key_file_path.unlink(missing_ok=True)

    # Generate a new key pair for the client
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    # Generate a certificate signing request (CSR) for the client
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, client_common_name)
    ])

    csr = x509.CertificateSigningRequestBuilder().subject_name(
        name=subject
    ).sign(
        private_key=private_key, algorithm=SHA256(), backend=default_backend()
    )

    # Load the CA certificate and key from disk
    with open(ca_cert_file_path, "rb") as f:
        ca_cert = x509.load_pem_x509_certificate(f.read(), default_backend())

    with open(ca_key_file_path, "rb") as f:
        ca_key = serialization.load_pem_private_key(f.read(), password=ca_password.encode() if ca_password else None, backend=default_backend())

    # Create a new certificate for the client, signed by the CA
    client_cert = x509.CertificateBuilder().subject_name(
        name=csr.subject
    ).issuer_name(
        name=ca_cert.subject
    ).public_key(
        key=csr.public_key()
    ).serial_number(
        number=x509.random_serial_number()
    ).not_valid_before(
        time=datetime.utcnow()
    ).not_valid_after(
        time=datetime.utcnow() + timedelta(days=365)
    ).add_extension(
        extval=x509.BasicConstraints(ca=False, path_length=None), critical=True
    ).sign(
        private_key=ca_key, algorithm=SHA256(), backend=default_backend()
    )

    # Write the client certificate and key to disk
    with open(client_cert_file_path, "wb") as f:
        f.write(client_cert.public_bytes(encoding=serialization.Encoding.PEM))
    with open(client_key_file_path, "wb") as f:
        if password:
            encryption_algorithm = serialization.BestAvailableEncryption(password.encode())
        else:
            encryption_algorithm = serialization.NoEncryption()

        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=encryption_algorithm
        ))

    print(f"Successfully created Client certificate keypair for Common Name (CN): '{client_common_name}'")
    print(f"Signed Client Certificate keypair with CA: '{ca_cert.subject}'s private key")
    print(f"Client Cert file: {client_cert_file_path.as_posix()}")
    print(f"Client Key file: {client_key_file_path.as_posix()}")

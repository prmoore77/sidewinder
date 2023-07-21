import OpenSSL
import os
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
def create_ca_keypair(ca_common_name: str,
                      ca_cert_file: str,
                      ca_key_file: str,
                      overwrite: bool
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
    key = OpenSSL.crypto.PKey()
    key.generate_key(OpenSSL.crypto.TYPE_RSA, 2048)

    # Generate a self-signed CA certificate
    ca_cert = OpenSSL.crypto.X509()
    ca_cert.get_subject().CN = ca_common_name
    ca_cert.set_version(CERTIFICATE_VERSION)
    ca_cert.set_serial_number(1000)
    ca_cert.gmtime_adj_notBefore(0)
    ca_cert.gmtime_adj_notAfter(365*24*60*60) # 1 year validity
    ca_cert.set_issuer(ca_cert.get_subject())
    ca_cert.set_pubkey(key)
    ca_cert.add_extensions([
        OpenSSL.crypto.X509Extension(b"basicConstraints", True, b"CA:TRUE"),
        OpenSSL.crypto.X509Extension(b"subjectKeyIdentifier", False, b"hash", subject=ca_cert),
    ])
    ca_cert.sign(key, "sha256")

    # Write the CA certificate and key to disk
    with open(Path(ca_cert_file_path), "wb") as f:
        f.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, ca_cert))
    with open(Path(ca_key_file_path), "wb") as f:
        f.write(OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, key))

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
def create_client_keypair(client_common_name: str,
                          ca_cert_file: str,
                          ca_key_file: str,
                          client_cert_file: str,
                          client_key_file: str,
                          overwrite: bool
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
    key = OpenSSL.crypto.PKey()
    key.generate_key(OpenSSL.crypto.TYPE_RSA, 2048)

    # Generate a certificate signing request (CSR) for the client
    req = OpenSSL.crypto.X509Req()
    req.get_subject().CN = client_common_name
    req.set_pubkey(key)
    req.sign(key, "sha256")

    # Load the CA certificate and key from disk
    with open(ca_cert_file_path, "rb") as f:
        ca_cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, f.read())
    with open(ca_key_file_path, "rb") as f:
        ca_key = OpenSSL.crypto.load_privatekey(OpenSSL.crypto.FILETYPE_PEM, f.read())

    # Create a new certificate for the client, signed by the CA
    client_cert = OpenSSL.crypto.X509()
    client_cert.set_version(CERTIFICATE_VERSION)
    client_cert.set_subject(req.get_subject())
    client_cert.set_serial_number(2000)
    client_cert.gmtime_adj_notBefore(0)
    client_cert.gmtime_adj_notAfter(365*24*60*60) # 1 year validity
    client_cert.set_issuer(ca_cert.get_subject())
    client_cert.set_pubkey(req.get_pubkey())
    client_cert.add_extensions([
        OpenSSL.crypto.X509Extension(b"basicConstraints", True, b"CA:FALSE"),
        OpenSSL.crypto.X509Extension(b"subjectKeyIdentifier", False, b"hash", subject=client_cert),
    ])
    client_cert.sign(ca_key, "sha256")

    # Write the client certificate and key to disk
    with open(client_cert_file_path, "wb") as f:
        f.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, client_cert))
    with open(client_key_file_path, "wb") as f:
        f.write(OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, key))

    print(f"Successfully created Client certificate keypair for Common Name (CN): '{client_common_name}'")
    print(f"Signed Client Certificate keypair with CA: '{ca_cert.get_subject()}'''s private key")
    print(f"Client Cert file: {client_cert_file_path.as_posix()}")
    print(f"Client Key file: {client_key_file_path.as_posix()}")

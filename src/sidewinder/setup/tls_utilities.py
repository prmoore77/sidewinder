# Taken from https://nachtimwald.com/2019/11/14/python-self-signed-cert-gen/
import socket
from pathlib import Path
import click

# Constants
TLS_DIR = Path("tls")
DEFAULT_CERT_FILE = (TLS_DIR / "server.crt").as_posix()
DEFAULT_KEY_FILE = (TLS_DIR / "server.key").as_posix()


def _gen_openssl():
    import random
    from OpenSSL import crypto

    pkey = crypto.PKey()
    pkey.generate_key(crypto.TYPE_RSA, 2048)

    x509 = crypto.X509()
    subject = x509.get_subject()
    subject.commonName = socket.gethostname()
    x509.set_issuer(subject)
    x509.gmtime_adj_notBefore(0)
    x509.gmtime_adj_notAfter(5 * 365 * 24 * 60 * 60)
    x509.set_pubkey(pkey)
    x509.set_serial_number(random.randrange(100000))
    x509.set_version(2)
    x509.add_extensions([
        crypto.X509Extension(b'subjectAltName', False,
                             ','.join([
                                 'DNS:%s' % socket.gethostname(),
                                 'DNS:*.%s' % socket.gethostname(),
                                 'DNS:localhost',
                                 'DNS:*.localhost']).encode()),
        crypto.X509Extension(b"basicConstraints", True, b"CA:false")])

    x509.sign(pkey, 'SHA256')

    return (crypto.dump_certificate(crypto.FILETYPE_PEM, x509),
            crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))


def gen_self_signed_cert():
    '''
    Returns (cert, key) as ASCII PEM strings
    '''
    return _gen_openssl()


def create_tls_keypair(cert_file: str = DEFAULT_CERT_FILE,
                       key_file: str = DEFAULT_KEY_FILE,
                       overwrite: bool = False
                       ):
    cert_file_path = Path(cert_file)
    key_file_path = Path(key_file)

    if cert_file_path.exists() or key_file_path.exists():
        if not overwrite:
            raise RuntimeError(f"The TLS Cert file(s): '{cert_file_path.as_posix()}' or '{key_file_path.as_posix()}' - exist - and overwrite is False, aborting.")
        else:
            cert_file_path.unlink(missing_ok=True)
            key_file_path.unlink(missing_ok=True)

    cert, key = gen_self_signed_cert()

    cert_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file=cert_file_path, mode="wb") as cert_file:
        cert_file.write(cert)

    key_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file=key_file_path, mode="wb") as key_file:
        key_file.write(key)

    print("Created TLS Key pair successfully.")
    print(f"Cert file path: {cert_file_path.as_posix()}")
    print(f"Key file path: {key_file_path.as_posix()}")


@click.command()
@click.option(
    "--cert-file",
    type=str,
    default=DEFAULT_CERT_FILE,
    required=True,
    help="The TLS certificate file to create."
)
@click.option(
    "--key-file",
    type=str,
    default=DEFAULT_KEY_FILE,
    required=True,
    help="The TLS key file to create."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the cert/key if they exist?"
)
def main():
    create_tls_keypair(**locals())


if __name__ == '__main__':
    create_tls_keypair()

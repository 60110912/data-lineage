import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import logging
import click
from data_lineage.encrypt import utf8, sshHome, certName
import yaml



log = logging.getLogger("encrypt_password")
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


@click.command()
@click.option('--password', required=True, help='user password')
@click.option(
    '--log',
    'logLevel',
    default="ERROR",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    help='Log level. Default log level DEBUG')
@click.option(
    '--file',
    'nifi_config',
    required=True, help='Nifi config'
)
def main(password, logLevel, connect_config):
    """
    Функция выводит зашифрованный пароль для сохранения в конфигураторе.
    Параметр:
        password - шифруемый пароль
        logLevel - уровень логгирования для тестирования
    """
    logging.basicConfig(
        level=logLevel,
        format=FORMAT
    )
    log.info("Генерирую сертификат для пароля")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    log.info(f"Сохраняю сертификат в папки {sshHome}")
    with open(f'{sshHome}{certName}_private_key.pem', 'wb') as f:
        f.write(private_pem)

    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(f'{sshHome}{certName}_public_key.pem', 'wb') as f:
        f.write(public_pem)
    log.info("Загружаю имеющиеся сетрификаты с диска")
    with open(f'{sshHome}{certName}_private_key.pem', "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    with open(f'{sshHome}{certName}_public_key.pem', "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    log.info("Шифрую сетификатом пароль")

    encrypted = base64.b64encode(public_key.encrypt(
        password.encode('utf-8'),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    ))
    with open(connect_config, 'r') as file:
        config = yaml.load(file, yaml.SafeLoader)
    log.debug(config)
    config['nifi_config']['encrypPassword'] = utf8(encrypted)
    with open(connect_config, 'w') as file:
        yaml.dump(config, file)


if __name__ == "__main__":
    main()

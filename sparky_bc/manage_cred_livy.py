import json
import requests
from hdfs import Client

from cryptography.fernet import Fernet


class ManageCred:
    """
    The `ManageCred` class is responsible for managing credentials by encrypting and decrypting them using the Fernet encryption algorithm.
    It provides methods to encrypt credentials, create a session with authenticated credentials, and create a HDFS client with decrypted credentials.
    """

    def __init__(self):
        """
        Initializes the `ManageCred` class by generating a new encryption key using the Fernet encryption algorithm.
        """
        self.__k = Fernet.generate_key()

    def encrypt(self, username: str, password: str) -> bytes:
        """
        Encrypts the provided username and password using the encryption key and returns the encrypted credentials.

        Args:
            username (str): The username to be encrypted.
            password (str): The password to be encrypted.

        Returns:
            bytes: The encrypted credentials.
        """
        f = Fernet(self.__k)
        msg = json.dumps({'username': username, 'password': password}).encode('utf8')
        encrypted = f.encrypt(msg)
        return encrypted

    def create_session(self, encrypted_credentials: bytes) -> requests.Session:
        """
        Decrypts the encrypted credentials using the encryption key and creates a session with authenticated credentials.

        Args:
            encrypted_credentials (bytes): The encrypted credentials.

        Returns:
            requests.Session: The session with authenticated credentials.
        """
        f = Fernet(self.__k)
        credentials = json.loads(f.decrypt(encrypted_credentials).decode('utf8'))
        session = requests.Session()
        session.auth = (credentials['username'], credentials['password'])
        return session

    def create_hdfs_client(self, web_hdfs: str, encrypted_credentials: bytes) -> Client:
        """
        Decrypts the encrypted credentials using the encryption key and creates a HDFS client with decrypted credentials.

        Args:
            web_hdfs (str): The URL of the HDFS server.
            encrypted_credentials (bytes): The encrypted credentials.

        Returns:
            Client: The HDFS client with decrypted credentials.
        """
        f = Fernet(self.__k)
        session =self.create_session(encrypted_credentials)
        client = Client(web_hdfs, session=session)
        return client



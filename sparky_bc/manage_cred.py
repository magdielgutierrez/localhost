import json

class ManageCred:
    def __init__(self):
        # Se pone en el init para que cryptography sea una librearia opcional igual que paramiko
        from cryptography.fernet import Fernet
        self.__k = Fernet.generate_key()
    
    def encrypt(self, username, password):
        from cryptography.fernet import Fernet
        f = Fernet(self.__k)
        msg = json.dumps({'username':username, 'password':password}).encode('utf8')
        encrypted = f.encrypt(msg)
        return encrypted
    
    def open(self, hostname, port, encrypted_credentials):
        import paramiko
        class Client(paramiko.SSHClient):
            def exec_command(
                self,
                command,
                bufsize=-1,
                timeout=None,
                get_pty=False,
                environment=None,
                ):

                command = self.__kinit(command, **json.loads(f.decrypt(encrypted_credentials).decode('utf8')))

                stdin, stdout, stderr = paramiko.SSHClient.exec_command(
                    self,
                    command=command,
                    bufsize=bufsize,
                    timeout=timeout,
                    get_pty=get_pty,
                    environment=environment,
                )

                return stdin, stdout, stderr

            def __kinit(self,
                        command,
                        username='',
                        password=''):
                symbols = [" " , "!" , '"' , "#" , "$" , "&" , "'" , "(" , ")" , "*" ,
                       "," , ";" , "<" , ">" , "?" , "[" , "]" , "^" , "`" ,
                       "{" , "|" , "}"]

                for s in symbols:
                    password = password.replace( s , "\\" + s)

                command = 'echo {} | kinit {}\n{}'.format(password, username, command)
                return command


        from cryptography.fernet import Fernet
        f = Fernet(self.__k)
        client =  Client()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostname, port=port, **json.loads(f.decrypt(encrypted_credentials).decode('utf8')))
        client.get_transport().set_keepalive(30)
        return client


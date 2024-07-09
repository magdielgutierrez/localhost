from .manage_cred import ManageCred

class Remote:
    def __init__(self, username, password):
        self.__mgr = ManageCred()
        self.__msg = self.__mgr.encrypt(username, password)

    def connect(self, hostname, port):
        import paramiko
        return self.__mgr.open(hostname, port, self.__msg)
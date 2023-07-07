import paramiko
import threading
import socket
import select
import uuid

all = ["SSHConnection"] 
class SSHConnection(object):
 
    def __init__(self, host='192.168.88.182', port=22, username='root',pwd='123456'):
        self.host = host
        self.port = port
        self.username = username
        self.pwd = pwd
        self.__k = None
 
    def connect(self):
        transport = paramiko.Transport((self.host,self.port))
        transport.connect(username=self.username,password=self.pwd)
        self.__transport = transport
 
    def close(self):
        self.__transport.close()
 
    def upload(self,local_path,target_path):
        # 连接，上传
        # file_name = self.create_file()
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        # 将location.py 上传至服务器 /tmp/test.py
        sftp.put(local_path, target_path)
 
    def download(self,remote_path,local_path):
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        sftp.get(remote_path,local_path)
        
    def reverse_forward_tunnel(self, server_port, remote_host, remote_port):
        def handler(chan, host, port):
            sock = socket.socket()
            try:
                sock.connect((host, port))
            except Exception as e:
                print('Forwarding request to %s:%d failed: %r' % (host, port, e))
                return
            
            print('Connected!  Tunnel open %r -> %r -> %r' % (chan.origin_addr,
                                                                chan.getpeername(), (host, port)))
            while True:
                r, w, x = select.select([sock, chan], [], [])
                if sock in r:
                    data = sock.recv(1024)
                    if len(data) == 0:
                        break
                    chan.send(data)
                if chan in r:
                    data = chan.recv(1024)
                    if len(data) == 0:
                        break
                    sock.send(data)
            chan.close()
            sock.close()
            print('Tunnel closed from %r' % (chan.origin_addr,))
        
        self.__transport.request_port_forward('', server_port)
        while True:
            chan = self.__transport.accept(1000)
            if chan is None:
                continue
            thr = threading.Thread(target=handler, args=(chan, remote_host, remote_port))
            thr.setDaemon(True)
            thr.start()
        
    def cmd(self, command):
        ssh = paramiko.SSHClient()
        ssh._transport = self.__transport
        # 执行命令
        stdin, stdout, stderr = ssh.exec_command(command)
        # 获取命令结果
        result = stdout.read()
        print (str(result,encoding='utf-8'))
        return result

if __name__ == "__main__":
    # SFTP服务器配置
    # config文件多账户批量配置
    sftp_para = {
        'host': '140.206.103.17',
        'port' : 22389,
        'username': "root",
        'pwd': 'KStest@88'
    }

    ssh = SSHConnection(host=sftp_para['host'], port=sftp_para['port'],
                        username=sftp_para['username'], pwd=sftp_para['pwd'])
    ssh.connect()
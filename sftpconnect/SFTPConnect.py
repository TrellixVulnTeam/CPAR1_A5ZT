import paramiko
from pprint import pprint
from secret import secret

class SFTPConnect:

    def __init__(self,sftpName,port=22,username="Anonymous",password=""):
        self.sftpName = sftpName
        self.transport = paramiko.Transport((sftpName, port))
        identifier = secret.getSFTP()
        if sftpName == identifier[0]:
            username=identifier[1]
            password=identifier[2]
        self.transport.connect(username = username, password = password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def mvGet(self,remotepath,localpath,close):
        '''Moves to a directory in SFTP, lists selection of files to grab '''
        self.sftp.chdir(remotepath)
        files = dict(enumerate(self.sftp.listdir()))
        files[-1] = 'quit'
        selection = None
        pprint(files)
        while selection != -1:
            selection = int(input("Which file do you want to get? (Select dictKey, -1 to quit) "))
            get_file = files[selection]
            if selection != -1:
                self.sftp.get(get_file,localpath+get_file)
                print("\n{} is now in {}".format(get_file, localpath.split('/')[-2]))
            print('\n')
        if close != True:
            print("Don't forget to close me")
        else:
            self.sftp.close()
            print("Connection closed")

    # def mvmkPut(self,remotepath,folder=None,files):
    #     self.sftp.chdir(remotepath)
    #     if folder != None:
    #         self.sftp.mkdir(folder)

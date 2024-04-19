import os
import json 
import re
import datetime
from abc import ABC, abstractmethod
import redis #- make this a dynamic import
#import sqlite - make this a dynamic import
import uuid
import hashlib
import subprocess

class ShellManager():
    
    def __init__(self,
                 ram_fs_mgr,
                 disk_fs_mgr):

        self.ram_fs_mgr = ram_fs_mgr
        self.disk_fs_mgr = disk_fs_mgr
        
    def ram_to_disk(self,disk_map_dict): 
        #disk_map_dict - Dict containing arguments about how to map the data to disk, following RESTful APIs
        exec("self.ram_fs_mgr.disk_map({})".format(disk_map_dict))
    
    def disk_to_ram(self,ram_map_dict): 
        exec("self.disk_fs_mgr.ram_map({})".format(ram_map_dict))

    def progress_bar(self,
                     percentage,
                     file,
                     file_idx,
                     total_files):

        bar_length = 100
        filled = int(bar_length * percentage // 100)
        bar = '[' + '=' * filled + '>' + ' ' * (bar_length - filled_length - 1) + ']'
        progress_msg = f"{bar} {percentage}% {file} [{file_idx}/{total_files}]"
        print(progress_bar,end = '\r')


    def parse_cmd(self,cmd):

        pattern = r"\s*(mkdir|cp|mv|ls|exit|touch|mkfile|rm|read|dump|load)\s*"
        matches = re.split(pattern,cmd.strip())[1:]

        if matches[0] == 'exit':
            exit()
        
        if matches[0] in ['mkfile','ls','read']:
            
            if matches[1][:6] == '/vdisk':
                exec('self.disk_fs_mgr.{}(matches[1].strip())'.format(matches[0]))
            else:
                exec('self.ram_fs_mgr.{}(matches[1].strip())'.format(matches[0]))
            
        if matches[0] == 'dump': 
            print('Disk -> RAM (1)\n')
            print('RAM -> Disk (2)\n')
            option = int(input('select an option above (1 or 2) ').strip())
            if option == 1:
                pass
            if option == 2:
                data_dict = self.ram_fs_mgr.dump(matches[1])
                #import pdb ; pdb.set_trace()
                print('successfully transferred data from RAMFS to Disk')

    def display_shell(self,shell_str = 'mafs_shell>:/'):

        
        prompt = f'{shell_str} '
        
        while True:
            try:
                user_input = input(prompt).strip()
                tokens = self.parse_cmd(user_input)
                
            except KeyboardInterrupt:
                print('exiting shell!')
                
        pass

    def fs_init(self,
                mode = 'file'):
       
        f = open('./info.txt','r').read()
        print(f)
        
        storage_mode = mode.upper()
        disk_dir = input('enter in a disk storage dir (leave blank for current): ').strip()
        if disk_dir == '':
            disk_dir = os.getcwd()
        if not os.path.exists(disk_dir):
            os.makedirs(disk_dir)


        print('initializing MAFS...')
        print(f"Storage mode: {storage_mode}\n")

        if mode == 'file':
            
            #initializing directories
            self.ram_fs_mgr.mkdir('/vram')
            self.ram_fs_mgr.mkdir('/vram/logs')
            self.ram_fs_mgr.mkdir('/vram/scripts')
            
            log_file_contents = 'ramfs_init = True\ndisk_dir = {}\n'.format(disk_dir,storage_mode)
            hashed_contents = self.ram_fs_mgr.hash_contents(log_file_contents)
            
            log_file_data_cfg = {'path': '/vram/logs/logfile.txt',
                                 'contents': log_file_contents,
                                 'uuid': str(uuid.uuid4())}

            
            log_file_metadata_cfg = {'owners': 'root',
                                    'timestamp': datetime.datetime.now().strftime("%Y-%m-&d %H:%M:%S.%f")} 
            
            log_file_cfg = {'data': log_file_data_cfg, 'metadata': log_file_metadata_cfg}
            self.ram_fs_mgr.write(cfg = log_file_cfg)

        #TODO
        if mode == 'block':
            pass

        #TODO
        if mode == 'object':
            pass

class DiskFSManager(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def snapshot(self,file):
        pass



class RAMFSManager(ABC): 

    @abstractmethod
    def __init__(self):
        pass
    
    @abstractmethod
    def mkdir(self):
        pass

    @abstractmethod
    def write(self,cfg): 
        pass
    
    @abstractmethod
    def ls(self):
        pass
    
    def cd(self):
        pass

    @abstractmethod    
    def read(self,cfg): 
        pass
    
    @abstractmethod
    def dump(self,disk_data_dict):
        pass

    def mkfile(self,
               file_name,
               text_editor = 'vim'):
        
        data_cfg = {'path': file_name,
                    'uuid': str(uuid.uuid4())}

        subprocess.run([f"{text_editor}",file_name.split('/')[-1]])

        contents = subprocess.run('cat {}'.format(file_name.split('/')[-1]),
                                  shell = True,
                                  capture_output = True).stdout.decode()
        
        data_cfg['contents'] = contents
        metadata_cfg = {'owners': 'root',
                        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}

        cfg = {'data': data_cfg,
               'metadata': metadata_cfg}

        self.write(cfg)
        subprocess.run('rm {}'.format(file_name.split('/')[-1]),
                       shell = True)
    
    
    def hash_contents(self,contents):
        return hashlib.sha256('{}'.format(contents).encode()).hexdigest()

    def parse_directory_path(self,path):
        tokens = path.strip().strip('.').split('/')
        return tokens

    
     
class RedisFSManager(RAMFSManager):
    
    def __init__(self,
                 redis_host = 'localhost',
                 redis_port = 6379):

        self.redis_server = redis.Redis(redis_host,
                                        redis_port)
        #self.cwd = '/'
        #self.redis_server.sadd('ramfs-dirs',self.cwd) #initialize directory structure by adding root
        
    def ls(self,
           path = '/',
           count = 10):
        
        if not path.endswith('/'):
            path += '/'

        pattern = f"{path}*"

        cursor = '0'
        children = set()
        while cursor != 0:
            cursor, keys = self.redis_server.scan(cursor,
                                                  match = pattern,
                                                  count = count)
            for key in keys: 
                relative_key = key[len(path):]
                child = relative_key.decode().split('/')[0]
                if child: 
                    children.add(child)
        children = list(children)
        for i in range(len(children)):
            print(children[i])

        

    def read(self,path):
        
        try:
            file_uuid = self.redis_server.get(path).decode() #fetches UUID
            contents = self.redis_server.hget(file_uuid,'contents')
            print('\n{}\n'.format(contents.decode()))
        except:
            print('file does not exist, are you sure you entered the full path correctly?')

    def mkdir(self,dir_name):
       
     
        tokens = self.parse_directory_path(dir_name)
        data = {'path': dir_name,
                'owners': 'root'} 
        dir_check = self.redis_server.get(data['path']) 
        self.redis_server.set(data['path'],
                              data['owners'])

    def dump(self,
             pattern):

        #creates the dicitonary to send to Manager
        data_dicts = []
        #optimize this with SCANs and pipelines
        all_keys = [key.decode('utf8') for key in self.redis_server.keys()]
        filtered_keys = [key for key in all_keys if (pattern in key)]
        for key in filtered_keys:
            if '.' in key: 
                file_uuid = self.redis_server.get(key)
                data_dicts.append(self.redis_server.hgetall(file_uuid))
        return data_dicts


    def write(self,cfg):
        
        #check & enforce RESTful API  
        #dir_ = '/'.join(cfg['data']['path'].split('/')[:-1])
        '''
        cfg = data
                - Path
                - UUID
                - contents
              metadata
                - owners
                - timestamp

        '''
        self.redis_server.set(cfg['data']['path'],
                              cfg['data']['uuid'])
        
        redis_hash_cfg = {'contents': cfg['data']['contents'],
                          'owners': cfg['metadata']['owners'],
                          'timestamp': cfg['metadata']['timestamp']}

        self.redis_server.hset(cfg['data']['uuid'],
                               'contents',
                               cfg['data']['contents'])
        
        self.redis_server.hset(cfg['data']['uuid'],
                               'owners',
                               cfg['metadata']['owners'])

        self.redis_server.hset(cfg['data']['uuid'],
                               'timestamp',
                               cfg['metadata']['timestamp'])

class SQLiteFSManager(RAMFSManager):

    def write(self,cfg): 
        pass

class BasicDiskManager(DiskFSManager):

    def __init__(self,
                 vdisk):
        
        self.vdisk = vdisk 
    
    def ls(self,
           count = 10):
        
        vdisk_contents = os.listdir(self.vdisk)
        #import pdb ; pdb.set_trace()
        for contents in vdisk_contents: 
            print(f'{contents}\n')

    def mkfile(self,
               file_name,
               text_editor = 'vim'): 
        
        dir_path, file_name = os.path.split(file_name)
        os.makedirs(dir_path,
                    exist_ok = True)
        #import pdb ; pdb.set_trace()
        subprocess.run('sudo {} {}/{}'.format(text_editor,
                                              self.vdisk,
                                              file_name.strip()),
                       shell = True)


    
    def mkdir(self,dir_name):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)


    def read(self,path):

        vdisk_name = self.vdisk #gets the actual name
        mapped_path = path.replace('/vdisk/',f'{self.vdisk}/')
        full_path = os.path.join(mapped_path)
        contents = open(full_path,'rb').read().decode('utf8')
        print(f'{contents}')

        
    def ram_to_disk(self):
        pass

    def disk_to_ram(self):
        pass

    def snapshot(self,file):
        pass

mgr = ShellManager(ram_fs_mgr = RedisFSManager(), disk_fs_mgr = BasicDiskManager(vdisk = 'block_dir'))
mgr.fs_init()
mgr.display_shell()
